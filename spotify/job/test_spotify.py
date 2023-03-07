import datetime as dt
import unittest

from job.dates import DateRange
from job.spotify import get_episode_release_date

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

if __name__ == '__main__':
    unittest.main()
