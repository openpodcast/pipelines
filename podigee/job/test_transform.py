import unittest
from job.__main__ import transform_podigee_analytics_to_metrics


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


if __name__ == "__main__":
    unittest.main()