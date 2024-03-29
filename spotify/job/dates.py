from typing import Tuple
import datetime as dt
from loguru import logger
import sys


class DateRange:
    """
    Represents a range of dates.
    """

    def __init__(self, start: dt.datetime, end: dt.datetime) -> None:
        self.start = start
        self.end = end

        # Calculate the number of days between start and end date
        self.days = (end - start).days

    def __iter__(self):
        """
        Iterate over all days in the range.
        """
        for i in range(self.days + 1):
            yield self.start + dt.timedelta(days=i)

    def chunks(self, days_per_chunk: int) -> Tuple[dt.datetime, dt.datetime]:
        """
        Iterate over all days in the range in chunks of `days_per_chunk`
        The chunks should return (start, start+days_per_chunk), (start+days_per_chunk, start+2*days_per_chunk), etc.
        until (start+days_per_chunk*n, end) and not go beyond the end date.
        """
        for i in range(0, self.days, days_per_chunk):
            start = self.start + dt.timedelta(days=i)
            end = min(self.start + dt.timedelta(days=i + days_per_chunk), self.end)
            yield (start, end)

    def __str__(self) -> str:
        """
        Return a string representation of the date range.
        """
        return f"Date range: {self.start} - {self.end} ({self.days} days)"


def try_convert_date(date: str) -> dt.datetime:
    """
    Convert a date string to a datetime object.
    This immediately exits the program if the date is invalid.
    """
    try:
        date = dt.datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        logger.error(
            f"Date is not in the correct format. Should be %Y-%m-%d, but is {date}. Quitting"
        )
        sys.exit(1)

    return date


def get_date_range(start_date: str, end_date: str) -> DateRange:
    """
    Convert start and end date to datetime objects.
    """

    start = try_convert_date(start_date)
    end = try_convert_date(end_date)

    if start > end:
        logger.error(
            f"Invalid date range: End date is before start date. (Start: {start}, End: {end}) Quitting"
        )
        raise Exception("Invalid date range")

    return DateRange(start, end)
