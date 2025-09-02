"""
Tests for date utility functions.
These tests can run independently without environment setup.
"""
import unittest
from datetime import datetime, date
from job.date_utils import (
    extract_date_str_from_iso,
    get_date_string,
    get_end_date_on_granularity
)


class TestDateUtils(unittest.TestCase):
    """Test date utility functions."""

    def test_extract_date_str_from_iso_with_z_suffix(self):
        """Test extracting date from ISO string with Z suffix."""
        iso_string = "2025-08-15T14:30:45.123Z"
        result = extract_date_str_from_iso(iso_string)
        self.assertEqual(result, "2025-08-15")

    def test_extract_date_str_from_iso_with_timezone_offset(self):
        """Test extracting date from ISO string with timezone offset."""
        iso_string = "2025-08-15T14:30:45+02:00"
        result = extract_date_str_from_iso(iso_string)
        self.assertEqual(result, "2025-08-15")

    def test_extract_date_str_from_iso_without_timezone(self):
        """Test extracting date from ISO string without timezone."""
        iso_string = "2025-08-15T14:30:45"
        result = extract_date_str_from_iso(iso_string)
        self.assertEqual(result, "2025-08-15")

    def test_extract_date_str_from_iso_date_only(self):
        """Test extracting date from date-only string."""
        iso_string = "2025-08-15"
        result = extract_date_str_from_iso(iso_string)
        self.assertEqual(result, "2025-08-15")

    def test_extract_date_str_from_iso_empty_string(self):
        """Test extracting date from empty string."""
        result = extract_date_str_from_iso("")
        self.assertEqual(result, "")

    def test_extract_date_str_from_iso_none(self):
        """Test extracting date from None."""
        result = extract_date_str_from_iso(None)
        self.assertEqual(result, "")

    def test_extract_date_str_from_iso_invalid_format(self):
        """Test extracting date from invalid format falls back to split."""
        iso_string = "invalid-2025-08-15T14:30:45-format"
        result = extract_date_str_from_iso(iso_string)
        # Should fall back to split method
        self.assertEqual(result, "invalid-2025-08-15")

    def test_extract_date_str_from_iso_no_t_separator(self):
        """Test extracting date from string without T separator."""
        iso_string = "not-a-datetime-string"
        result = extract_date_str_from_iso(iso_string)
        # Should return the string as-is when no T separator
        self.assertEqual(result, "not-a-datetime-string")

    def test_get_date_string_from_string(self):
        """Test get_date_string with string input."""
        date_str = "2025-08-15"
        result = get_date_string(date_str)
        self.assertEqual(result, "2025-08-15")

    def test_get_date_string_from_datetime(self):
        """Test get_date_string with datetime object."""
        dt = datetime(2025, 8, 15, 14, 30, 45)
        result = get_date_string(dt)
        self.assertEqual(result, "2025-08-15")

    def test_get_date_string_from_date(self):
        """Test get_date_string with date object."""
        d = date(2025, 8, 15)
        result = get_date_string(d)
        self.assertEqual(result, "2025-08-15")

    def test_get_date_string_from_number(self):
        """Test get_date_string with number input."""
        result = get_date_string(20250815)
        self.assertEqual(result, "20250815")

    def test_get_end_date_on_granularity_day_with_string(self):
        """Test get_end_date_on_granularity with day granularity and string input."""
        start_date = "2025-08-15"
        result = get_end_date_on_granularity("day", start_date)
        self.assertEqual(result, "2025-08-15")

    def test_get_end_date_on_granularity_day_with_datetime(self):
        """Test get_end_date_on_granularity with day granularity and datetime input."""
        start_date = datetime(2025, 8, 15, 14, 30, 45)
        result = get_end_date_on_granularity("day", start_date)
        self.assertEqual(result, "2025-08-15")

    def test_get_end_date_on_granularity_month_string_start_of_month(self):
        """Test get_end_date_on_granularity with month granularity from start of month."""
        start_date = "2025-08-01"
        result = get_end_date_on_granularity("month", start_date)
        self.assertEqual(result, "2025-08-31")

    def test_get_end_date_on_granularity_month_string_mid_month(self):
        """Test get_end_date_on_granularity with month granularity from mid month."""
        start_date = "2025-08-15"
        result = get_end_date_on_granularity("month", start_date)
        self.assertEqual(result, "2025-08-31")

    def test_get_end_date_on_granularity_month_datetime_object(self):
        """Test get_end_date_on_granularity with month granularity and datetime object."""
        start_date = datetime(2025, 8, 15, 14, 30, 45)
        result = get_end_date_on_granularity("month", start_date)
        self.assertEqual(result, "2025-08-31")

    def test_get_end_date_on_granularity_month_february_leap_year(self):
        """Test get_end_date_on_granularity with February in leap year."""
        start_date = "2024-02-15"  # 2024 is a leap year
        result = get_end_date_on_granularity("month", start_date)
        self.assertEqual(result, "2024-02-29")

    def test_get_end_date_on_granularity_month_february_non_leap_year(self):
        """Test get_end_date_on_granularity with February in non-leap year."""
        start_date = "2025-02-15"  # 2025 is not a leap year
        result = get_end_date_on_granularity("month", start_date)
        self.assertEqual(result, "2025-02-28")

    def test_get_end_date_on_granularity_unknown_granularity(self):
        """Test get_end_date_on_granularity with unknown granularity."""
        start_date = "2025-08-15"
        result = get_end_date_on_granularity("unknown", start_date)
        # Should fall back to returning the start date
        self.assertEqual(result, "2025-08-15")

    def test_get_end_date_on_granularity_month_december(self):
        """Test get_end_date_on_granularity with December."""
        start_date = "2025-12-01"
        result = get_end_date_on_granularity("month", start_date)
        self.assertEqual(result, "2025-12-31")

    def test_get_end_date_on_granularity_month_april(self):
        """Test get_end_date_on_granularity with April (30 days)."""
        start_date = "2025-04-10"
        result = get_end_date_on_granularity("month", start_date)
        self.assertEqual(result, "2025-04-30")


if __name__ == "__main__":
    unittest.main()
