import queue

import requests
from loguru import logger

from fetch_params import FetchParams
from open_podcast import OpenPodcastConnector


def worker(q: queue.Queue, openpodcast: OpenPodcastConnector) -> None:
    """
    A worker thread that fetches data from the Spotify API
    """
    while True:
        params = q.get()
        fetch(openpodcast, params)
        q.task_done()


def fetch(openpodcast: OpenPodcastConnector, params: FetchParams) -> None:
    """
    Fetches data from the Spotify API and sends it to the Open Podcast API
    """
    try:
        data = params.spotify_call()
        openpodcast.post(params.meta, data, params.start, params.end)
    except requests.exceptions.HTTPError as e:
        logger.error(e)
        return
