import queue
from time import sleep

import requests
from loguru import logger

from job.fetch_params import FetchParams
from job.open_podcast import OpenPodcastConnector

# Retry configuration
MAX_RETRIES = 5
INITIAL_BACKOFF = 1  # seconds
MAX_BACKOFF = 60  # seconds


def worker(q: queue.Queue, openpodcast: OpenPodcastConnector, delay) -> None:
    """
    A worker thread that fetches data from the Spotify API
    """
    while True:
        params = q.get()
        fetch(openpodcast, params)
        q.task_done()
        sleep(delay)


def fetch(openpodcast: OpenPodcastConnector, params: FetchParams) -> None:
    """
    Fetches data from the Spotify API and sends it to the Open Podcast API
    """
    retry_count = 0
    backoff = INITIAL_BACKOFF
    
    while retry_count <= MAX_RETRIES:
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
            # Success - exit retry loop
            return
        except requests.exceptions.HTTPError as e:
            # Check if it's a rate limit error (429) or server error (5xx)
            if e.response is not None and (e.response.status_code == 429 or e.response.status_code >= 500):
                if retry_count < MAX_RETRIES:
                    retry_count += 1
                    logger.warning(
                        f"HTTP {e.response.status_code} error for {params.openpodcast_endpoint}. "
                        f"Retry {retry_count}/{MAX_RETRIES} after {backoff}s backoff. Error: {e}"
                    )
                    sleep(backoff)
                    backoff = min(backoff * 2, MAX_BACKOFF)
                else:
                    logger.error(
                        f"Max retries ({MAX_RETRIES}) reached for {params.openpodcast_endpoint}. "
                        f"Final error: {e}"
                    )
                    return
            else:
                # For other HTTP errors (4xx except 429), don't retry
                logger.error(f"HTTP error for {params.openpodcast_endpoint}: {e}")
                return
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            # Network errors - retry with backoff
            if retry_count < MAX_RETRIES:
                retry_count += 1
                logger.warning(
                    f"Network error for {params.openpodcast_endpoint}. "
                    f"Retry {retry_count}/{MAX_RETRIES} after {backoff}s backoff. Error: {e}"
                )
                sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF)
            else:
                logger.error(
                    f"Max retries ({MAX_RETRIES}) reached for {params.openpodcast_endpoint}. "
                    f"Final network error: {e}"
                )
                return
        except Exception as e:
            # Unexpected errors - log and don't retry
            logger.error(f"Unexpected error for {params.openpodcast_endpoint}: {e}")
            return
