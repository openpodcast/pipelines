import unittest
from job.transforms import (
    transform_podigee_analytics_to_metrics,
    transform_podigee_podcast_overview,
    extract_date_str_from_iso
)


class TestTransformPodigeeAnalyticsToMetrics(unittest.TestCase):
    def setUp(self):
        """Set up test data matching the format described in the issue"""
        self.sample_analytics_data = {
            'meta': {
                'timerange': {'start_datetime': 1753920000, 'end_datetime': 1756598399}, 
                'aggregation_granularity': 'day'
            }, 
            'objects': [{
                'downloaded_on': '2025-07-31T00:00:00Z', 
                'downloads': {'complete': 100}, 
                'formats': {'mp3': 100}, 
                'platforms': {'iPhone': 64, 'Android': 32, 'Windows': 1, 'unknown': 3}, 
                'countries': {'AL': 1, 'AT': 35, 'CH': 4, 'DE': 54, 'DK': 1, 'FR': 2, 'IT': 1, 'PT': 1, 'TH': 1}, 
                'clients': {'Chrome': 1, 'Deezer': 1, 'Podimo': 1, 'Audible': 5, 'CastBox': 2, 'Spotify': 37, 'AntennaPod': 1, 'Amazon Music': 6, 'Chrome Mobile': 2, 'Mobile Safari': 1, 'Apple Podcasts': 43}
            }]
        }
        
        self.sample_overview_data = {
            "published_episodes_count": 10,
            "audio_published_minutes": 500.0,
            "unique_listeners_number": 305,
            "unique_subscribers_number": 283,
            "total_downloads": 694.0,
            "meta": {
                "from": "2025-08-01T00:00:00.000Z",
                "to": "2025-08-31T23:59:59.999Z"
            }
        }

    def test_transform_with_countries_processing(self):
        """Test that countries are now being processed"""
        result = transform_podigee_analytics_to_metrics(self.sample_analytics_data, store_downloads_only=False)
        
        # Check that we have metrics
        self.assertIn('metrics', result)
        metrics = result['metrics']
        self.assertGreater(len(metrics), 0)
        
        # Check that we have downloads, platforms, clients, and countries
        dimensions = {metric['dimension'] for metric in metrics}
        self.assertIn('downloads', dimensions)
        self.assertIn('platforms', dimensions) 
        self.assertIn('clients', dimensions)
        self.assertIn('countries', dimensions)
        
        # Verify specific country metrics are present
        country_metrics = [m for m in metrics if m['dimension'] == 'countries']
        self.assertGreater(len(country_metrics), 0)
        
        # Check that specific countries from our test data are present
        country_codes = {m['subdimension'] for m in country_metrics}
        self.assertIn('DE', country_codes)  # Germany with 54 downloads
        self.assertIn('AT', country_codes)  # Austria with 35 downloads
        
        # Verify the values are correct
        de_metric = next(m for m in country_metrics if m['subdimension'] == 'DE')
        self.assertEqual(de_metric['value'], 54)
        
        at_metric = next(m for m in country_metrics if m['subdimension'] == 'AT')
        self.assertEqual(at_metric['value'], 35)

    def test_transform_with_store_downloads_only(self):
        """Test that when store_downloads_only=True, only downloads are processed"""
        result = transform_podigee_analytics_to_metrics(self.sample_analytics_data, store_downloads_only=True)
        
        # Check that we have metrics
        self.assertIn('metrics', result)
        metrics = result['metrics']
        self.assertGreater(len(metrics), 0)
        
        # Should only have downloads dimension
        dimensions = {metric['dimension'] for metric in metrics}
        self.assertEqual(dimensions, {'downloads'})

    def test_countries_data_exists_in_sample(self):
        """Verify our test data has countries as described in the issue"""
        countries = self.sample_analytics_data['objects'][0]['countries']
        self.assertIn('DE', countries)
        self.assertEqual(countries['DE'], 54)
        self.assertIn('AT', countries)
        self.assertEqual(countries['AT'], 35)

    def test_empty_data_handling(self):
        """Test that empty or malformed data is handled gracefully"""
        # Test with None
        result = transform_podigee_analytics_to_metrics(None)
        self.assertEqual(result, {"metrics": []})
        
        # Test with empty dict
        result = transform_podigee_analytics_to_metrics({})
        self.assertEqual(result, {"metrics": []})
        
        # Test with missing objects
        result = transform_podigee_analytics_to_metrics({"meta": {}})
        self.assertEqual(result, {"metrics": []})

    def test_overview_transform(self):
        """Test the podcast overview transformation"""
        result = transform_podigee_podcast_overview(self.sample_overview_data)
        
        self.assertIn('metrics', result)
        metrics = result['metrics']
        self.assertEqual(len(metrics), 3)  # listeners, subscribers, downloads
        
        # Check listeners metric
        listeners_metric = next(m for m in metrics if m['dimension'] == 'listeners')
        self.assertEqual(listeners_metric['value'], 305)
        self.assertEqual(listeners_metric['subdimension'], 'unique')
        self.assertEqual(listeners_metric['start'], '2025-08-01')
        self.assertEqual(listeners_metric['end'], '2025-08-31')
        
        # Check subscribers metric
        subscribers_metric = next(m for m in metrics if m['dimension'] == 'subscribers')
        self.assertEqual(subscribers_metric['value'], 283)
        
        # Check downloads metric
        downloads_metric = next(m for m in metrics if m['dimension'] == 'downloads')
        self.assertEqual(downloads_metric['value'], 694.0)

    def test_overview_empty_data(self):
        """Test overview transform with empty data"""
        result = transform_podigee_podcast_overview(None)
        self.assertEqual(result, {"metrics": []})
        
        result = transform_podigee_podcast_overview({})
        self.assertEqual(result, {"metrics": []})

    def test_extract_date_str_from_iso(self):
        """Test the ISO date extraction function"""
        # Test normal ISO string with Z
        result = extract_date_str_from_iso("2025-07-31T00:00:00Z")
        self.assertEqual(result, "2025-07-31")
        
        # Test ISO string with timezone offset
        result = extract_date_str_from_iso("2025-07-31T02:00:00+02:00")
        self.assertEqual(result, "2025-07-31")
        
        # Test empty string
        result = extract_date_str_from_iso("")
        self.assertEqual(result, "")
        
        # Test None
        result = extract_date_str_from_iso(None)
        self.assertEqual(result, "")
        
        # Test malformed string (should fallback to split)
        result = extract_date_str_from_iso("2025-07-31T00:00:00")
        self.assertEqual(result, "2025-07-31")

    def test_analytics_with_monthly_granularity(self):
        """Test analytics transform with monthly granularity"""
        monthly_data = {
            'meta': {'aggregation_granularity': 'month'}, 
            'objects': [{
                'downloaded_on': '2025-07-01T00:00:00Z',
                'downloads': {'complete': 3000}
            }]
        }
        
        result = transform_podigee_analytics_to_metrics(monthly_data, store_downloads_only=True)
        metrics = result['metrics']
        
        # Should have one download metric
        self.assertEqual(len(metrics), 1)
        metric = metrics[0]
        self.assertEqual(metric['start'], '2025-07-01')
        self.assertEqual(metric['end'], '2025-07-31')  # Last day of July
        self.assertEqual(metric['value'], 3000)


if __name__ == "__main__":
    unittest.main()