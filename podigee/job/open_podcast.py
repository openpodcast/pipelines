import datetime as dt
import types
import requests
from loguru import logger


class OpenPodcastConnector:
    """
    Client for Open Podcast API.
    """

    def __init__(self, url: str, token: str, podcast_id: str):
        self.url = url
        self.token = token
        self.headers = {"Authorization": f"Bearer {self.token}"}
        self.default_meta = {
            "show": podcast_id,
        }

    def merge_meta(self, endpoint: str, extra_meta: dict):
        """
        Merge meta data with default meta data.
        """
        meta = {
            **self.default_meta,
            "endpoint": endpoint,
        }
        if extra_meta:
            meta = {
                **meta,
                **extra_meta,
            }
        return meta

    def post(self, endpoint, extra_meta, data, start, end):
        """
        Send POST request to Open Podcast API.
        """
        if extra_meta and "episode" in extra_meta:
            logger.info(
                f"Storing `{endpoint}` [{start} - {end}] for episode {extra_meta['episode']}"
            )
        else:
            logger.info(f"Storing `{endpoint}` [{start} - {end}]")

        meta = self.merge_meta(endpoint, extra_meta)

        # If the data is a generator, we need convert it to a list with
        # `endpoint_name` as the key (e.g. for `episodes` and `detailedStreams`)
        if isinstance(data, types.GeneratorType):
            data = {endpoint: list(data)}

        payload = {
            "provider": "podigee",
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
            f"{self.url}/connector", headers=self.headers, json=payload, timeout=60
        )

        # log error if response is not 200
        if response.status_code != 200:
            logger.error(
                f"Failed to store `{endpoint}` [{start} - {end}] with status code {response.status_code} and response {response.text}"
            )

        return response

    def health(self):
        """
        Send GET request to the Open Podcast healthcheck endpoint `/health`.
        """
        logger.info(f"Checking health of {self.url}/health")
        return requests.get(f"{self.url}/health", timeout=60)
