from __future__ import annotations

import queue
from dataclasses import dataclass
from datetime import datetime
from time import sleep
from typing import Any, Callable

import requests
from loguru import logger

from job.open_podcast import OpenPodcastConnector


@dataclass
class FetchParams:
    """Parameters for a single API call."""

    openpodcast_endpoint: str
    call: Callable[[], Any]
    start_date: datetime
    end_date: datetime
    meta: dict | None = None


def worker(q: queue.Queue, openpodcast: OpenPodcastConnector, delay: float) -> None:
    """Worker thread: drain the queue, posting each result to the Open Podcast API."""
    while True:
        params: FetchParams = q.get()
        _fetch(openpodcast, params)
        q.task_done()
        sleep(delay)


def _fetch(openpodcast: OpenPodcastConnector, params: FetchParams) -> None:
    try:
        data = params.call()
        if data is not None:
            openpodcast.post(
                params.openpodcast_endpoint,
                params.meta or {},
                data,
                params.start_date,
                params.end_date,
            )
    except requests.exceptions.HTTPError as exc:
        logger.error("HTTP error for {}: {}", params.openpodcast_endpoint, exc)
    except Exception as exc:
        logger.error("Unexpected error for {}: {}", params.openpodcast_endpoint, exc)
