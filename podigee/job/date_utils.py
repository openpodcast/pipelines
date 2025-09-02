"""
Date utility functions for Podigee data processing.
"""
from datetime import datetime
import calendar


def extract_date_str_from_iso(iso_string):
    """
    Extract date string (YYYY-MM-DD) from ISO datetime string.
    Since Podigee always sends UTC timestamps with 'Z', this preserves the UTC date.
    """
    if not iso_string:
        return ""
    try:
        # Python 3.11+ handles 'Z' suffix directly
        dt = datetime.fromisoformat(iso_string)
        return dt.strftime("%Y-%m-%d")
    except (ValueError, AttributeError):
        # Fallback to split method if parsing fails
        return iso_string.split("T")[0] if "T" in iso_string else iso_string


def get_date_string(date_obj):
    """
    Convert date object to string if needed, or return string as-is.
    """
    if isinstance(date_obj, str):
        return date_obj
    elif isinstance(date_obj, datetime):
        return date_obj.strftime("%Y-%m-%d")
    elif hasattr(date_obj, 'strftime'):  # handles date objects too
        return date_obj.strftime("%Y-%m-%d")
    else:
        return str(date_obj)


def get_end_date_on_granularity(granularity, start_date):
    """
    Get end date based on granularity and start date.
    Returns a string in YYYY-MM-DD format.
    """
    if granularity == "day":
        return get_date_string(start_date)
    elif granularity == "month":
        # Convert to datetime object if needed
        if isinstance(start_date, str):
            date_obj = datetime.strptime(start_date, "%Y-%m-%d")
        else:
            date_obj = start_date
        
        # Get last day of the month
        last_day = calendar.monthrange(date_obj.year, date_obj.month)[1]
        end_date = date_obj.replace(day=last_day)
        return get_date_string(end_date)
    return get_date_string(start_date)
