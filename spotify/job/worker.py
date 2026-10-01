import math
import queue
import threading
from time import sleep

from loguru import logger

from job.fetch_params import FetchParams
from job.open_podcast import OpenPodcastConnector


class EmptyListenerData(ValueError):
    """The source returned no daily show-listener rows to persist."""


def worker(q: queue.Queue, openpodcast: OpenPodcastConnector, delay, failures) -> None:
    """Drain tasks even after failures, recording them for the main thread."""
    while True:
        params = q.get()
        try:
            if params is None:
                return
            try:
                fetch(openpodcast, params)
            except Exception as exc:
                failures.put(params)
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
            sleep(delay)
        finally:
            q.task_done()


def run_tasks(endpoints, openpodcast, delay, num_workers) -> int:
    """Finish all queued tasks and return the number that failed."""
    if num_workers < 1 or not math.isfinite(delay) or delay < 0:
        raise ValueError(
            "NUM_WORKERS must be positive and TASK_DELAY finite and non-negative"
        )
    tasks = queue.Queue()
    failures = queue.Queue()
    threads = [
        threading.Thread(target=worker, args=(tasks, openpodcast, delay, failures))
        for _ in range(num_workers)
    ]
    for endpoint in endpoints:
        tasks.put(endpoint)
    for _ in threads:
        tasks.put(None)
    for thread in threads:
        thread.start()
    tasks.join()
    for thread in threads:
        thread.join()
    return failures.qsize()


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
