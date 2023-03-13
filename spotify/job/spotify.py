from loguru import logger
import datetime as dt

from job.dates import DateRange
from job.dates import get_date_range


def get_episode_release_date(episode):
    """
    Returns the release date of an episode as a datetime object
    """
    if not "releaseDate" in episode:
        logger.warning(f"Episode {episode['id']} has no release date. Continuing...")
        return None
    try:
        return dt.datetime.strptime(episode["releaseDate"], "%Y-%m-%d")
    except ValueError:
        logger.warning(
            f"Episode {episode['id']} has an invalid release date ({episode['releaseDate']}). Continuing..."
        )
        return None


def get_episode_date_range(episode, global_date_range: DateRange) -> DateRange:
    """
    Returns a list of date ranges for an episode. The date ranges are
    calculated based on the release date of the episode and the start
    and end date of the time range.
    """

    release_date = get_episode_release_date(episode)
    if not release_date:
        # Return original date range if episode has no release date
        return global_date_range

    # Start at the release date of the episode or the start date of the time
    # range, whichever is later
    start_date = max(release_date, global_date_range.start)
    end_date = global_date_range.end

    return get_date_range(
        start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
    )
