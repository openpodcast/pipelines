import queue
from time import sleep

import requests
from loguru import logger

from job.fetch_params import FetchParams
from job.open_podcast import OpenPodcastConnector


def worker(q: queue.Queue, openpodcast: OpenPodcastConnector) -> None:
    """
    A worker thread that fetches data from the Anchor API
    """
    while True:
        params = q.get()
        fetch(openpodcast, params)
        q.task_done()


def fetch(openpodcast: OpenPodcastConnector, params: FetchParams) -> None:
    """
    Fetches data from the Anchor API and sends it to the Open Podcast API
    """
    try:
        data = params.anchor_call()
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
