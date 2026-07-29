import datetime as dt

import requests
from loguru import logger

GENDER_KEYS = ("FEMALE", "MALE", "NON_BINARY", "NOT_SPECIFIED")
AGE_KEYS = ("28-34", "0-17", "45-59", "60-150", "23-27", "18-22", "35-44", "unknown")


def _zero_gender_counts():
    return {gender: 0 for gender in GENDER_KEYS}


def empty_aggregate(start, end):
    return {
        "count": 0,
        "start": start.strftime("%Y-%m-%d"),
        "end": end.strftime("%Y-%m-%d"),
        "ageFacetedCounts": {
            age: {"counts": _zero_gender_counts()} for age in AGE_KEYS
        },
        "countryFacetedCounts": {},
        "genderedCounts": {"counts": _zero_gender_counts()},
    }


def aggregate_or_empty(spotify_call, start, end):
    try:
        return spotify_call()
    except requests.exceptions.HTTPError as error:
        response = getattr(error, "response", None)
        if response is not None and response.status_code == 500:
            logger.warning(
                f"Spotify aggregate returned 500 for {start.strftime('%Y-%m-%d')} - {end.strftime('%Y-%m-%d')}; sending zero aggregate"
            )
            return empty_aggregate(start, end)
        raise


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
        return dt.datetime.strptime(episode["releaseDate"], "%Y-%m-%d")  # noqa: DTZ007
    except ValueError:
        logger.warning(
            f"Episode {episode['id']} has an invalid release date ({episode['releaseDate']}). Continuing..."
        )
        return None
