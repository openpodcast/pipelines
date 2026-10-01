import math
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from time import sleep

from loguru import logger

from job.fetch_params import FetchParams
from job.open_podcast import OpenPodcastConnector


class EmptyListenerData(ValueError):
    """The source returned no daily show-listener rows to persist."""


def _run_task(
    params: FetchParams, openpodcast: OpenPodcastConnector, delay: float
) -> int:
    try:
        fetch(openpodcast, params)
        return 0
    except Exception as exc:
        # Exceptions/responses may contain credentials or request payloads.
        logger.error(
            "Failed `{}` [{} - {}] episode={}: {} (HTTP {})",
            params.openpodcast_endpoint,
            params.start_date,
            params.end_date,
            (params.meta or {}).get("episode", "show"),
            type(exc).__name__,
            getattr(getattr(exc, "response", None), "status_code", None),
        )
        return 1
    finally:
        sleep(delay)


def run_tasks(
    endpoints: Iterable[FetchParams],
    openpodcast: OpenPodcastConnector,
    delay: float,
    num_workers: int,
) -> int:
    """Finish all tasks and return the number that failed."""
    if not math.isfinite(delay) or delay < 0:
        raise ValueError("TASK_DELAY must be finite and non-negative")
    with ThreadPoolExecutor(max_workers=num_workers) as pool:
        return sum(
            pool.map(
                partial(_run_task, openpodcast=openpodcast, delay=delay), endpoints
            )
        )


def fetch(openpodcast: OpenPodcastConnector, params: FetchParams) -> None:
    """Fetch and store one task; let the worker report failures."""
    data = params.spotify_call()
    if params.openpodcast_endpoint == "listeners" and not (params.meta or {}).get(
        "episode"
    ):
        # Missing data is not evidence of zero listeners. Explicit zero entries
        # are valid, but an empty series would otherwise succeed without a DB write.
        if (
            not isinstance(data, dict)
            or not isinstance(data.get("counts"), list)
            or not data["counts"]
        ):
            raise EmptyListenerData("Show listener response has no daily counts")
    if data:
        openpodcast.post(
            params.openpodcast_endpoint,
            params.meta,
            data,
            params.start_date,
            params.end_date,
        )
