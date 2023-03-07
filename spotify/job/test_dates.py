import datetime as dt
import unittest

from job.dates import DateRange

class TestDateRange(unittest.TestCase):
    def setUp(self):
        self.start = dt.datetime(2023, 1, 1)
        self.end = dt.datetime(2023, 1, 5)
        self.daterange = DateRange(self.start, self.end)

    def test_iterator(self):
        expected_output = [(dt.datetime(2023, 1, 1), dt.datetime(2023, 1, 5)),
                           (dt.datetime(2022, 12, 31), dt.datetime(2023, 1, 4)),
                           (dt.datetime(2022, 12, 30), dt.datetime(2023, 1, 3)),
                           (dt.datetime(2022, 12, 29), dt.datetime(2023, 1, 2))]
        self.assertEqual(list(self.daterange), expected_output)

    def test_string_representation(self):
        expected_output = "[2023-01-01 00:00:00 - 2023-01-05 00:00:00]"
        self.assertEqual(str(self.daterange), expected_output)

if __name__ == '__main__':
    unittest.main()