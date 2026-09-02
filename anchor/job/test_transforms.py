import unittest

from job.transforms import (
    get_top_geo,
    transform_episodes_page,
    transform_plays_by_age_range,
    transform_plays_by_app,
    transform_plays_by_device,
    transform_plays_by_gender,
    transform_plays_by_geo,
    transform_plays_by_geo_region,
    transform_total_plays_by_episode,
)


def analytics_response(field, value):
    return {
        "showByShowUri": {
            field: {"analyticsValue": {"analyticsValue": value}},
            "uri": "spotify:show:test",
        }
    }


class TestPlatformTransforms(unittest.TestCase):
    def test_transform_plays_by_app_uses_current_response_field(self):
        graphql_data = analytics_response(
            "showPlaysAndDownloadsByApp",
            {"apps": [{"displayName": "Spotify", "value": 0.75}]},
        )

        transformed = transform_plays_by_app(graphql_data)

        self.assertEqual(transformed["data"]["rows"], [["Spotify", 0.75]])

    def test_transform_plays_by_device_uses_spotify_colors(self):
        graphql_data = analytics_response(
            "showPlaysAndDownloadsByDevice",
            {"devices": [{"color": "#5D55FF", "displayName": "iPhone", "value": 0.38}]},
        )

        transformed = transform_plays_by_device(graphql_data)

        self.assertEqual(transformed["data"]["rows"], [["iPhone", 0.38]])
        self.assertEqual(transformed["data"]["colors"], {"iPhone": "#5D55FF"})


class TestGeoTransforms(unittest.TestCase):
    def setUp(self):
        self.graphql_data = analytics_response(
            "showPlaysAndDownloadsByGeo",
            {
                "geos": [
                    {
                        "displayName": "Austria",
                        "flagUrl": "https://example.com/at.svg",
                        "navigationName": "AT",
                        "value": 0.66,
                    }
                ]
            },
        )

    def test_extracts_navigation_name_for_drill_down(self):
        self.assertEqual(
            get_top_geo(self.graphql_data),
            {
                "displayName": "Austria",
                "flagUrl": "https://example.com/at.svg",
                "navigationName": "AT",
                "value": 0.66,
            },
        )

    def test_transform_plays_by_geo_uses_current_field_and_flags(self):
        transformed = transform_plays_by_geo(self.graphql_data)

        self.assertEqual(transformed["data"]["rows"], [["Austria", 0.66]])
        self.assertEqual(
            transformed["data"]["assets"]["flagUrlByGeo"],
            {"Austria": "https://example.com/at.svg"},
        )

    def test_transform_plays_by_geo_region_uses_current_field(self):
        transformed = transform_plays_by_geo_region(
            self.graphql_data, country="Austria"
        )

        self.assertEqual(transformed["parameters"]["geos"], [None, "Austria", None])
        self.assertEqual(transformed["data"]["rows"], [["Austria", 0.66]])

    def test_handles_missing_geo_data(self):
        graphql_data = {"showByShowUri": {"showPlaysAndDownloadsByGeo": {}}}

        self.assertIsNone(get_top_geo(graphql_data))
        self.assertEqual(transform_plays_by_geo(graphql_data)["data"]["rows"], [])


class TestDemographicTransforms(unittest.TestCase):
    def test_transform_plays_by_age_range_includes_required_legacy_buckets(self):
        graphql_data = {
            "showByShowUri": {
                "showPlaysByAge": {
                    "analyticsValue": {
                        "analyticsValue": {
                            "ageBreakdown": [],
                            "totalValue": 0,
                        }
                    }
                }
            }
        }

        transformed = transform_plays_by_age_range(graphql_data)
        required_buckets = {"0-17", "18-22", "23-27", "28-34", "35-44", "45-59", "60+"}

        self.assertEqual(
            set(transformed["data"]["translationMapping"].keys()), required_buckets
        )
        self.assertEqual(set(transformed["data"]["colors"].keys()), required_buckets)

    def test_transform_plays_by_gender_handles_null_gender_breakdown(self):
        graphql_data = {
            "showByShowUri": {
                "showPlaysByGender": {
                    "analyticsValue": {
                        "analyticsValue": {
                            "genderBreakdown": None,
                        }
                    }
                }
            }
        }

        transformed = transform_plays_by_gender(graphql_data)
        self.assertEqual(transformed["data"]["rows"], [])

    def test_transforms_current_age_and_gender_responses(self):
        graphql_data = {
            "showByShowUri": {
                "showPlaysByAge": {
                    "analyticsValue": {
                        "analyticsValue": {
                            "ageBreakdown": [
                                {
                                    "ageBracket": "23-27",
                                    "displayName": "23-27",
                                    "genderBreakdown": {"counts": [], "total": 95},
                                },
                                {
                                    "ageBracket": "60-150",
                                    "displayName": "60+",
                                    "genderBreakdown": {"counts": [], "total": 35},
                                },
                            ],
                            "totalValue": 349,
                        }
                    }
                },
                "showPlaysByGender": {
                    "analyticsValue": {
                        "analyticsValue": {
                            "genderBreakdown": {
                                "counts": [
                                    {
                                        "color": "#26008D",
                                        "displayName": "Male",
                                        "percent": 0.149,
                                    }
                                ],
                                "total": 349,
                            },
                            "totalValue": 349,
                        }
                    }
                },
            }
        }

        age = transform_plays_by_age_range(graphql_data)
        gender = transform_plays_by_gender(graphql_data)

        self.assertEqual(
            age["data"]["rows"],
            [["23-27", 95 / 349], ["60+", 35 / 349]],
        )
        self.assertEqual(gender["data"]["rows"], [["Male", 0.149]])
        self.assertEqual(gender["data"]["colors"], {"Male": "#26008D"})


class TestEpisodeListTransform(unittest.TestCase):
    def test_transforms_current_native_episode_item(self):
        episodes = [
            {
                "episodeId": 122814255,
                "uri": "spotify:episode:test",
                "title": "Episode title",
                "episodeType": "EPISODE_TYPE_FULL",
                "contentType": "EPISODE_CONTENT_TYPE_AUDIO",
                "publishedOn": {"seconds": 1784070000},
                "createdOn": {"seconds": 1784018878},
                "asset": {
                    "lengthMs": 1003477,
                    "downloadUrl": "https://example.com/episode.mp3",
                    "mediaFiles": [{"mediaType": "MEDIA_TYPE_AUDIO"}],
                },
                "analyticsStreamsAndDownloads": {
                    "analyticsValue": {"analyticsValue": {"value": 284}}
                },
                "analyticsPlaysAndDownloads": {
                    "analyticsValue": {"analyticsValue": {"value": 307}}
                },
            }
        ]

        transformed = transform_episodes_page(
            episodes,
            legacy_web_ids_by_uri={"spotify:episode:test": "e123"},
        )

        self.assertEqual(len(transformed), 1)
        self.assertEqual(transformed[0]["episodeId"], 122814255)
        self.assertEqual(transformed[0]["webEpisodeId"], "e123")
        self.assertEqual(transformed[0]["totalPlays"], 307)
        self.assertEqual(transformed[0]["duration"], 1003477)
        self.assertEqual(transformed[0]["audioCount"], 1)

    def test_total_plays_by_episode_uses_plays_and_downloads_and_ranks(self):
        episodes = [
            {
                "episodeId": 122814255,
                "uri": "spotify:episode:first",
                "title": "First episode",
                "publishedOn": {"seconds": 1784070000},
                "analyticsStreamsAndDownloads": {
                    "analyticsValue": {"analyticsValue": {"value": 160}}
                },
                "analyticsPlaysAndDownloads": {
                    "analyticsValue": {"analyticsValue": {"value": 307}}
                },
            },
            {
                "episodeId": 122814256,
                "uri": "spotify:episode:second",
                "title": "Second episode",
                "publishedOn": {"seconds": 1784156400},
                "analyticsPlaysAndDownloads": {
                    "analyticsValue": {"analyticsValue": {"value": 410}}
                },
            },
        ]

        transformed = transform_total_plays_by_episode(episodes)

        self.assertEqual(
            transformed["data"]["rows"],
            [
                [
                    "Second episode",
                    122814256,
                    410,
                    1784156400,
                    1,
                    "spotify:episode:second",
                ],
                [
                    "First episode",
                    122814255,
                    307,
                    1784070000,
                    2,
                    "spotify:episode:first",
                ],
            ],
        )


if __name__ == "__main__":
    unittest.main()
