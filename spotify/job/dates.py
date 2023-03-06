import datetime as dt
from loguru import logger
import sys


def try_convert_date(date: str) -> dt.datetime:
    try:
        date = dt.datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        logger.error(
            f"Date is not in the correct format. Should be %Y-%m-%d, but is {date}. Quitting"
        )
        sys.exit(1)

    return date


def try_convert_dates(
    start_date: str, end_date: str
) -> tuple[dt.datetime, dt.datetime, int]:
    """
    Convert start and end date to datetime objects.
    This immediately exits the program if the dates are invalid.
    """

    start_date = try_convert_date(start_date)
    end_date = try_convert_date(end_date)

    if start_date > end_date:
        logger.error("Invalid date range: End date is before start date. Quitting")
        sys.exit(1)

    # Calculate the number of days between start and end date
    days_diff_start_end = (end_date - start_date).days

    return (start_date, end_date, days_diff_start_end)
