import datetime as dt
import types
import requests
from loguru import logger


class OpenPodcastConnector:
    """
    Client for Open Podcast API.
    """

    def __init__(self, endpoint: str, token: str, podcast_id: str):
        self.endpoint = endpoint
        self.token = token
        self.headers = {"Authorization": f"Bearer {self.token}"}
        self.default_meta = {
            "show": podcast_id,
        }

    def post(self, meta, data, start, end):
        """
        Send POST request to Open Podcast API.
        """
        logger.info(f"Sending data for {meta.endpoint} to Open Podcast API")

        # If the data is a generator, we need convert it to a list with
        # `endpoint_name` as the key (e.g. for `episodes` and `detailedStreams`)
        if isinstance(data, types.GeneratorType):
            data = {meta.endpoint: list(data)}

        # Merge meta data
        meta = {
            **self.default_meta,
            **meta,
        }

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

        return requests.post(
            f"{self.endpoint}/connector", headers=self.headers, json=payload, timeout=60
        )

    def health(self):
        """
        Send GET request to the Open Podcast healthcheck endpoint `/health`.
        """
        logger.info(f"Checking health of {self.endpoint}/health")
        return requests.get(f"{self.endpoint}/health", timeout=60)
