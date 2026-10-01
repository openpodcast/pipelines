import datetime as dt
import types
from email.utils import parsedate_to_datetime
from time import sleep
import requests
from loguru import logger


# These API handlers key rows by dates in the payload. Snapshot handlers such
# as metadata/performance use server "today" and are not safe to replay at midnight.
RETRYABLE_ENDPOINTS = {
    "listeners",
    "detailedStreams",
    "followers",
    "aggregate",
    "impressions_total",
    "impressions_faceted",
    "impressions_daily",
}


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

        # Materialize once and replay only endpoints with payload-derived keys.
        attempts = 3 if endpoint in RETRYABLE_ENDPOINTS else 1
        for attempt in range(attempts):
            try:
                response = requests.post(
                    f"{self.url}/connector",
                    headers=self.headers,
                    json=payload,
                    timeout=60,
                    allow_redirects=False,
                )
            except (requests.ConnectionError, requests.Timeout):
                if attempt == attempts - 1:
                    raise
                logger.warning(
                    "Retrying ingestion `{}` after transport failure (attempt {}/3)",
                    endpoint,
                    attempt + 1,
                )
                sleep(2**attempt)
                continue

            if response.status_code == 200:
                return response
            if (
                response.status_code not in (429, 500, 502, 503, 504)
                or attempt == attempts - 1
            ):
                response.close()
                # Do not include response bodies, headers, or bearer tokens.
                raise requests.HTTPError(
                    f"Ingestion `{endpoint}` failed with HTTP {response.status_code}",
                    response=response,
                )
            delay = retry_delay(response.headers.get("Retry-After"), 2**attempt)
            logger.warning(
                "Retrying ingestion `{}` after HTTP {} in {}s (attempt {}/3)",
                endpoint,
                response.status_code,
                delay,
                attempt + 1,
            )
            response.close()
            sleep(delay)

    def health(self):
        """
        Send GET request to the Open Podcast healthcheck endpoint `/health`.
        """
        logger.info(f"Checking health of {self.url}/health")
        return requests.get(f"{self.url}/health", timeout=60)


def retry_delay(retry_after, default):
    """Honor Retry-After seconds/HTTP dates, capped at 60 seconds per retry."""
    if retry_after:
        try:
            seconds = int(retry_after)
        except ValueError:
            try:
                retry_at = parsedate_to_datetime(retry_after)
                seconds = (retry_at - dt.datetime.now(dt.timezone.utc)).total_seconds()
            except (TypeError, ValueError, OverflowError):
                return default
        return min(60, max(default, seconds))
    return default
