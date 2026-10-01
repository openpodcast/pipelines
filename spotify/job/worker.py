from time import sleep

from loguru import logger

from job.fetch_params import FetchParams
from job.open_podcast import OpenPodcastConnector


def fetch(openpodcast: OpenPodcastConnector, params: FetchParams, delay=0) -> bool:
    """Fetch and store one task, reporting whether it succeeded."""
    try:
        data = params.spotify_call()
        if (
            params.openpodcast_endpoint == "listeners"
            and not (params.meta or {}).get("episode")
            and (not data or not data.get("counts"))
        ):
            raise ValueError("Show listener response has no daily counts")
        if data:
            openpodcast.post(
                params.openpodcast_endpoint,
                params.meta,
                data,
                params.start_date,
                params.end_date,
            )
        return True
    except Exception as exc:
        # Avoid logging exception text, which may contain response data or secrets.
        logger.error(
            "Failed `{}` [{} - {}] episode={}: {}",
            params.openpodcast_endpoint,
            params.start_date,
            params.end_date,
            (params.meta or {}).get("episode", "show"),
            type(exc).__name__,
        )
        return False
    finally:
        sleep(delay)
