import datetime as dt

import requests
from loguru import logger


class OpenPodcastConnector:
    """Client for the Open Podcast API."""

    def __init__(self, url: str, token: str, show_uri: str):
        self.url = url
        self.token = token
        self.headers = {"Authorization": f"Bearer {token}"}
        self.default_meta = {"show": show_uri}

    def merge_meta(self, endpoint: str, extra_meta: dict) -> dict:
        meta = {**self.default_meta, "endpoint": endpoint}
        if extra_meta:
            meta = {**meta, **extra_meta}
        return meta

    def post(
        self,
        endpoint: str,
        extra_meta: dict,
        data,
        start: dt.datetime,
        end: dt.datetime,
    ):
        if extra_meta and "episode" in extra_meta:
            logger.info(
                "Storing `{}` [{} - {}] for episode {}",
                endpoint,
                start.strftime("%Y-%m-%d"),
                end.strftime("%Y-%m-%d"),
                extra_meta["episode"],
            )
        else:
            logger.info(
                "Storing `{}` [{} - {}]",
                endpoint,
                start.strftime("%Y-%m-%d"),
                end.strftime("%Y-%m-%d"),
            )

        meta = self.merge_meta(endpoint, extra_meta)
        payload = {
            "provider": "spotify",
            "version": 1,
            "retrieved": dt.datetime.now().isoformat(),
            "meta": meta,
            "range": {
                "start": start.strftime("%Y-%m-%d"),
                "end": end.strftime("%Y-%m-%d"),
            },
            "data": data,
        }

        response = requests.post(
            f"{self.url}/connector",
            headers=self.headers,
            json=payload,
            timeout=60,
        )
        if response.status_code != 200:
            logger.error(
                "Failed to store `{}` [{} - {}]: HTTP {} - {}",
                endpoint,
                start.strftime("%Y-%m-%d"),
                end.strftime("%Y-%m-%d"),
                response.status_code,
                response.text,
            )
        return response

    def health(self):
        logger.info("Checking health of {}/health", self.url)
        return requests.get(f"{self.url}/health", timeout=60)
