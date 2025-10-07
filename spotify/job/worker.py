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
http_session = requests.Session()
http_session.mount("http://", adapter)
http_session.mount("https://", adapter)


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
    # Temporarily replace requests module functions with our session
    original_get = requests.get
    original_post = requests.post
    original_request = requests.request
    
    try:
        # Monkey-patch requests to use our retry session
        requests.get = http_session.get
        requests.post = http_session.post  
        requests.request = http_session.request
        
        # Make the API call
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
    finally:
        # Restore original requests functions
        requests.get = original_get
        requests.post = original_post
        requests.request = original_request
