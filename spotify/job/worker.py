import queue
from time import sleep

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from loguru import logger

from job.fetch_params import FetchParams
from job.open_podcast import OpenPodcastConnector


# Configure retry strategy for HTTP requests
# This will retry on rate limit (429) and server errors (5xx)
retry_strategy = Retry(
    total=5,  # Maximum number of retry attempts
    backoff_factor=1,  # Exponential backoff: 1s, 2s, 4s, 8s, 16s
    status_forcelist=[429, 500, 502, 503, 504],  # Retry on these HTTP status codes
    allowed_methods=["HEAD", "GET", "POST", "PUT", "DELETE", "OPTIONS", "TRACE"],
)

# Create HTTP adapter with retry strategy
adapter = HTTPAdapter(max_retries=retry_strategy)

# Create a global session with the retry adapter configured
# This session will be used for all HTTP requests in this module
http_session = requests.Session()
http_session.mount("http://", adapter)
http_session.mount("https://", adapter)

# Replace requests module functions with our session methods
# This makes all requests in this module use the retry logic
requests.get = http_session.get
requests.post = http_session.post
requests.put = http_session.put
requests.delete = http_session.delete
requests.patch = http_session.patch
requests.head = http_session.head
requests.options = http_session.options
requests.request = http_session.request


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
    Fetches data from the Spotify API and sends it to the Open Podcast API.
    Uses HTTPAdapter with retry strategy for automatic retries on rate limits and server errors.
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
        logger.error(e)
        return
