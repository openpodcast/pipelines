import datetime as dt
import unittest

from job.spotify import get_episode_release_date, normalize_performance


class TestNormalizePerformance(unittest.TestCase):
    def test_adds_missing_fields_without_mutating_response(self):
        response = {"samples": [0.9, 0.8, 0.7], "episode": "episode-id"}

        normalized = normalize_performance(response)

        self.assertEqual(
            normalized,
            {
                "samples": [0.9, 0.8, 0.7],
                "episode": "episode-id",
                "sampleRate": 1000,
                "seconds": 3,
            },
        )
        self.assertIsNot(normalized, response)
        self.assertEqual(
            response, {"samples": [0.9, 0.8, 0.7], "episode": "episode-id"}
        )

    def test_preserves_supplied_fields(self):
        responses = [
            (
                {"samples": [0.9, 0.8], "sampleRate": 500},
                {"sampleRate": 500, "seconds": 2},
            ),
            (
                {"samples": [0.9, 0.8], "seconds": 12},
                {"sampleRate": 1000, "seconds": 12},
            ),
        ]

        for response, expected in responses:
            with self.subTest(response=response):
                normalized = normalize_performance(response)

                self.assertEqual(normalized["sampleRate"], expected["sampleRate"])
                self.assertEqual(normalized["seconds"], expected["seconds"])
                self.assertIsNot(normalized, response)

    def test_leaves_malformed_responses_unchanged(self):
        responses = [
            None,
            [0.9, 0.8],
            {"error": "missing samples"},
            {"samples": "not a list"},
        ]

        for response in responses:
            with self.subTest(response=response):
                self.assertIs(normalize_performance(response), response)


class TestGetEpisodeReleaseDate(unittest.TestCase):
    def setUp(self):
        self.episode1 = {"id": 1, "releaseDate": "2022-03-10"}
        self.episode2 = {"id": 2, "releaseDate": "2022-02-28"}
        self.episode3 = {"id": 3, "releaseDate": "invalid date"}
        self.episode4 = {"id": 4}

    def test_get_release_date_valid_date(self):
        result = get_episode_release_date(self.episode1)
        expected = dt.datetime(2022, 3, 10)
        self.assertEqual(result, expected)

    def test_get_release_date_invalid_date(self):
        result = get_episode_release_date(self.episode3)
        self.assertIsNone(result)

    def test_get_release_date_no_date(self):
        result = get_episode_release_date(self.episode4)
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
