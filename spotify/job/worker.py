import queue
from time import sleep

import requests
from loguru import logger
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

from job.fetch_params import FetchParams
from job.open_podcast import OpenPodcastConnector


def worker(q: queue.Queue, openpodcast: OpenPodcastConnector, delay) -> None:
    """
    A worker thread that fetches data from the Spotify API
    """
    while True:
        params = q.get()
        fetch(openpodcast, params)
        q.task_done()
        sleep(delay)


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=60),
    retry=(
        retry_if_exception_type(requests.exceptions.HTTPError)
        | retry_if_exception_type(requests.exceptions.ConnectionError)
        | retry_if_exception_type(requests.exceptions.Timeout)
    ),
    before_sleep=before_sleep_log(logger, "WARNING"),
    reraise=True,
)
def fetch(openpodcast: OpenPodcastConnector, params: FetchParams) -> None:
    """
    Fetches data from the Spotify API and sends it to the Open Podcast API.
    Uses tenacity retry decorator for automatic retries on rate limits and network errors.
    """
    try:
        data = params.spotify_call()
        if data:
            openpodcast.post(
                params.openpodcast_endpoint,
                params.meta,
                data,
                params.start_date,
                params.end_date,
            )
    except requests.exceptions.HTTPError as e:
        # Check if it's a retryable error (429, 5xx)
        if e.response is not None and (e.response.status_code == 429 or e.response.status_code >= 500):
            logger.warning(f"Retryable HTTP error {e.response.status_code} for {params.openpodcast_endpoint}")
            raise  # Re-raise to trigger retry
        else:
            # Non-retryable client error (4xx except 429)
            logger.error(f"Non-retryable HTTP error for {params.openpodcast_endpoint}: {e}")
            return
