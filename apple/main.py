import os
import datetime as dt
import json
import sys
import time
from loguru import logger
from appleconnector import AppleConnector, Metric, Dimension
import requests
import types
import itertools

PODCAST_ID = os.environ.get("PODCAST_ID")
OPENPODCAST_API_ENDPOINT = os.environ.get("OPENPODCAST_API_ENDPOINT")
# OPENPODCAST_API_ENDPOINT = "http://localhost:8080/connector"
OPENPODCAST_API_TOKEN = os.environ.get("OPENPODCAST_API_TOKEN")
APPLE_AUTOMATION_ENDPOINT = os.environ.get("APPLE_AUTOMATION_ENDPOINT")

# Store data locally for debugging. If this is set to `False`,
# data will only be sent to Open Podcast API.
# Load from environment variable if set, otherwise default to 0
STORE_DATA = os.environ.get("STORE_DATA", "False") == "True"


class OpenPodcastApi:
    def __init__(self, endpoint, token):
        self.endpoint = endpoint
        self.token = token
        pass

    def capture(self, data, range, meta={}):
        """
        Send POST request to Open Podcast API.
        """
        headers = {"Authorization": f"Bearer {self.token}"}
        json = {
            "provider": "apple",
            "version": 1,
            "retrieved": dt.datetime.now().isoformat(),
            "meta": meta,
            "range": range,
            "data": data,
        }
        return requests.post(f"{self.endpoint}/connector", headers=headers, json=json)

    def health(self):
        """
        Send GET request to the Open Podcast healthcheck endpoint `/health`.
        """
        logger.info(f"Checking health of {self.endpoint}/health")
        return requests.get(f"{self.endpoint}/health")


def get_cookies():
    """
    Get cookies from API
    """
    response = requests.get(APPLE_AUTOMATION_ENDPOINT, timeout=600)

    logger.info(f"Got cookies response: {response.status_code}")
    if response.status_code != 200:
        raise Exception(f"Failed to get cookies: {response.text}")

    cookies = response.json()
    return cookies


def fetch_and_capture(
    endpoint_name,
    file_path_prefix,
    connector_call,
    open_podcast_client,
    start,
    end,
    extra_meta={},
    fallible=False,
):
    """
    Wrapper function to fetch data from Apple and directly send to Open Podcast API.
    """
    logger.info(f"Fetching {endpoint_name}")
    try:
        data = connector_call()
    except Exception as e:
        logger.error(f"Failed to fetch data from {endpoint_name} endpoint: {e}")
        if fallible:
            # Silently ignore errors because for some endpoints we don't have
            # data
            return
        else:
            # Raise error if endpoint is not fallible (default)
            raise e

    if STORE_DATA:
        filename = f"{file_path_prefix}{dt.datetime.now()}.json"
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, "w+") as f:
            json.dump(data, f)

    result = open_podcast_client.capture(
        data,
        # Merge in extra metadata (if any)
        meta={
            **extra_meta,
            **{
                "show": PODCAST_ID,
                "endpoint": endpoint_name,
            },
        },
        range={
            "start": start.strftime("%Y-%m-%d"),
            "end": end.strftime("%Y-%m-%d"),
        },
    )
    logger.info(f"{endpoint_name}: {result.text}")

    return data


def api_healthcheck(open_podcast_client):
    """
    Try three times to get 200 from healthcheck endpoint
    """
    for i in range(3):
        status = open_podcast_client.health()
        if status.status_code == 200:
            return True
        else:
            logger.info(f"Healthcheck failed, retrying in 5 seconds...")
            time.sleep(5)
    return False


def main():
    # Call API which returns an array of cookies.
    # Structure of cookies is:
    # [
    #   {
    #     "name": "my-cookie-name",
    #     "value": "my-cookie-value",
    #     ...
    #   },
    #   ...
    # ]

    print("Getting Apple cookies")
    cookies = get_cookies()

    # Get myacinfo cookie
    myacinfo_cookie = next(c for c in cookies if c["name"] == "myacinfo")
    myacinfo = myacinfo_cookie["value"]

    # Get itctx cookie
    itctx_cookie = next(c for c in cookies if c["name"] == "itctx")
    itctx = itctx_cookie["value"]

    logger.info(f"Got cookies: {myacinfo}, {itctx}")

    apple_connector = AppleConnector(
        podcast_id=PODCAST_ID,
        myacinfo=myacinfo,
        itctx=itctx,
    )
    open_podcast_client = OpenPodcastApi(
        endpoint=OPENPODCAST_API_ENDPOINT,
        token=OPENPODCAST_API_TOKEN,
    )

    # Check if API is up before sending data
    if not api_healthcheck(open_podcast_client):
        logger.error("Open Podcast API is not up. Quitting")
        sys.exit(1)

    start = dt.datetime.now() - dt.timedelta(days=1)
    end = dt.datetime.now()
    fetch_and_capture(
        "overview",
        "data/podcast/overview/",
        lambda: apple_connector.overview(),
        open_podcast_client,
        start,
        end,
    )

    end = dt.datetime.now()
    start = dt.datetime.now() - dt.timedelta(days=7)
    fetch_and_capture(
        "trends",
        f"data/podcast/trends/{Metric.FOLLOWERS}/",
        lambda: apple_connector.trends(start, end, metric=Metric.FOLLOWERS),
        open_podcast_client,
        start,
        end,
        extra_meta={
            "metric": Metric.FOLLOWERS,
        },
    )

    end = dt.datetime.now()
    start = dt.datetime.now() - dt.timedelta(days=7)
    fetch_and_capture(
        "trends",
        f"data/podcast/trends/{Metric.LISTENERS}/{Dimension.BY_EPISODES}/",
        lambda: apple_connector.trends(
            start, end, metric=Metric.LISTENERS, dimension=Dimension.BY_EPISODES
        ),
        open_podcast_client,
        start,
        end,
        extra_meta={
            "metric": Metric.LISTENERS,
            "dimension": Dimension.BY_EPISODES,
        },
    )

    # Fetch podcast episodes
    #
    # Response:
    # {
    #   "content": {
    #     "results": {
    #       "1000584606852": {
    #         "id": "10005846068
    #         ...
    #       },
    #       ...
    #     }
    #   }
    # }
    end = dt.datetime.now()
    start = dt.datetime.now() - dt.timedelta(days=7)
    episodes = fetch_and_capture(
        "episodes",
        "data/podcast/episodes/",
        lambda: apple_connector.episodes(),
        open_podcast_client,
        start,
        end,
    )

    # Show error if no episodes are found
    if not episodes or not episodes["content"] or not episodes["content"]["results"]:
        logger.error("No episodes found")
        return

    for episode_id, _episode in episodes["content"]["results"].items():
        end = dt.datetime.now()
        start = dt.datetime.now() - dt.timedelta(days=7)
        fetch_and_capture(
            "episodeDetails",
            "data/episodes/details/",
            lambda: apple_connector.episode(episode_id),
            open_podcast_client,
            start,
            end,
            extra_meta={
                "episode": episode_id,
            },
        )


if __name__ == "__main__":
    main()
