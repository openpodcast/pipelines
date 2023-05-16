from loguru import logger
import datetime as dt

from job.dates import DateRange
from job.dates import get_date_range


def get_episode_release_date(episode):
    """
    Returns the release date of an episode as a datetime object
    """
    if not "releaseDate" in episode:
        logger.warning(
            f"Episode {episode['id']} has no release date. Continuing...")
        return None
    try:
        return dt.datetime.strptime(episode["releaseDate"], "%Y-%m-%d")
    except ValueError:
        logger.warning(
            f"Episode {episode['id']} has an invalid release date ({episode['releaseDate']}). Continuing..."
        )
        return None
