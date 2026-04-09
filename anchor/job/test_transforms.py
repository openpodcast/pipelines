import unittest

from job.transforms import transform_plays_by_age_range, transform_plays_by_gender


class TestDemographicTransforms(unittest.TestCase):
    def test_transform_plays_by_age_range_includes_required_legacy_buckets(self):
        graphql_data = {
            "showByShowUri": {
                "showStreamsFaceted": {
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

        self.assertEqual(set(transformed["data"]["translationMapping"].keys()), required_buckets)
        self.assertEqual(set(transformed["data"]["colors"].keys()), required_buckets)

    def test_transform_plays_by_gender_handles_null_gender_breakdown(self):
        graphql_data = {
            "showByShowUri": {
                "showStreamsFaceted": {
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


if __name__ == "__main__":
    unittest.main()
