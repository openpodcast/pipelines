from loguru import logger
import datetime as dt


def normalize_performance(data):
    if not isinstance(data, dict) or not isinstance(data.get("samples"), list):
        return data

    return {
        **data,
        "sampleRate": data.get("sampleRate", 1000),
        "seconds": data.get("seconds", len(data["samples"])),
    }


def get_episode_release_date(episode):
    """
    Returns the release date of an episode as a datetime object
    """
    if "releaseDate" not in episode:
        logger.warning(f"Episode {episode['id']} has no release date. Continuing...")
        return None
    try:
        return dt.datetime.strptime(episode["releaseDate"], "%Y-%m-%d")
    except ValueError:
        logger.warning(
            f"Episode {episode['id']} has an invalid release date ({episode['releaseDate']}). Continuing..."
        )
        return None
