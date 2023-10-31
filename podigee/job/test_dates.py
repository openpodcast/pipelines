import datetime as dt
import unittest
from typing import Tuple

from job.dates import DateRange
from job.dates import get_date_range


class TestDateRange(unittest.TestCase):
    def test_iterator(self):
        self.start = dt.datetime(2022, 1, 1)
        self.end = dt.datetime(2022, 1, 10)
        self.date_range = DateRange(self.start, self.end)
        expected_dates = [dt.datetime(2022, 1, i) for i in range(1, 11)]

        dates = list(self.date_range)
        self.assertEqual(dates, expected_dates)

    def test_chunks(self):
        self.start = dt.datetime(2022, 1, 1)
        self.end = dt.datetime(2022, 1, 11)
        self.date_range = DateRange(self.start, self.end)
        expected_chunks = [
            (dt.datetime(2022, 1, 1), dt.datetime(2022, 1, 4)),
            (dt.datetime(2022, 1, 4), dt.datetime(2022, 1, 7)),
            (dt.datetime(2022, 1, 7), dt.datetime(2022, 1, 10)),
            # Last chunk yields the remainder of the dates
            (dt.datetime(2022, 1, 10), dt.datetime(2022, 1, 11)),
        ]

        chunks = list(self.date_range.chunks(3))
        self.assertEqual(chunks, expected_chunks)


class TestGetDateRange(unittest.TestCase):
    def test_valid_date_range(self):
        start_date = "2022-01-01"
        end_date = "2022-01-10"
        date_range = get_date_range(start_date, end_date)

        self.assertEqual(date_range.start, dt.datetime(2022, 1, 1))
        self.assertEqual(date_range.end, dt.datetime(2022, 1, 10))
        self.assertEqual(date_range.days, 9)

    def test_invalid_date_range(self):
        start_date = "2022-01-10"
        end_date = "2022-01-01"

        with self.assertRaises(Exception):
            get_date_range(start_date, end_date)


if __name__ == "__main__":
    unittest.main()
