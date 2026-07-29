import datetime as dt
import unittest

import requests

from job.spotify import (
    aggregate_or_empty,
    empty_aggregate,
    get_episode_release_date,
    normalize_performance,
)


class TestAggregateOrEmpty(unittest.TestCase):
    def setUp(self):
        self.start = dt.datetime(2026, 7, 22, tzinfo=dt.UTC)
        self.end = dt.datetime(2026, 7, 28, tzinfo=dt.UTC)

    def test_empty_aggregate_matches_expected_shape(self):
        aggregate = empty_aggregate(self.start, self.end)

        self.assertEqual(aggregate["count"], 0)
        self.assertEqual(aggregate["start"], "2026-07-22")
        self.assertEqual(aggregate["end"], "2026-07-28")
        self.assertEqual(aggregate["countryFacetedCounts"], {})
        self.assertEqual(
            aggregate["genderedCounts"],
            {
                "counts": {
                    "FEMALE": 0,
                    "MALE": 0,
                    "NON_BINARY": 0,
                    "NOT_SPECIFIED": 0,
                }
            },
        )
        self.assertEqual(
            set(aggregate["ageFacetedCounts"]),
            {"28-34", "0-17", "45-59", "60-150", "23-27", "18-22", "35-44", "unknown"},
        )
        for value in aggregate["ageFacetedCounts"].values():
            self.assertEqual(value, aggregate["genderedCounts"])

    def test_returns_empty_aggregate_on_http_500(self):
        response = requests.Response()
        response.status_code = 500

        def spotify_call():
            raise requests.exceptions.HTTPError(response=response)

        self.assertEqual(
            aggregate_or_empty(spotify_call, self.start, self.end),
            empty_aggregate(self.start, self.end),
        )

    def test_reraises_non_500_http_errors(self):
        response = requests.Response()
        response.status_code = 404

        def spotify_call():
            raise requests.exceptions.HTTPError(response=response)

        with self.assertRaises(requests.exceptions.HTTPError):
            aggregate_or_empty(spotify_call, self.start, self.end)

    def test_returns_successful_response_unchanged(self):
        response = {"count": 1}

        self.assertIs(
            aggregate_or_empty(lambda: response, self.start, self.end),
            response,
        )


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
        expected = dt.datetime(2022, 3, 10)  # noqa: DTZ001
        self.assertEqual(result, expected)

    def test_get_release_date_invalid_date(self):
        result = get_episode_release_date(self.episode3)
        self.assertIsNone(result)

    def test_get_release_date_no_date(self):
        result = get_episode_release_date(self.episode4)
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
