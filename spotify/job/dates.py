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
        Iterate over all days in the range
        """
        for i in range(self.days):
            yield (self.start - dt.timedelta(days=i), self.end - dt.timedelta(days=i))


def try_convert_date(date: str) -> dt.datetime:
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
    This immediately exits the program if the dates are invalid.
    """

    start = try_convert_date(start_date)
    end = try_convert_date(end_date)

    if start > end:
        logger.error("Invalid date range: End date is before start date. Quitting")
        sys.exit(1)

    return DateRange(start, end)
