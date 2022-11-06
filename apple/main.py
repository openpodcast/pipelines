import os
import datetime as dt
import json
from loguru import logger
from appleconnector import AppleConnector
import requests
import types

PODCAST_ID = os.environ.get("PODCAST_ID")
MYACINFO = os.environ.get("MYACINFO")
ITCTX = os.environ.get("ITCTX")
OPENPODCAST_API_ENDPOINT = "https://api.openpodcast.dev/connector"
# OPENPODCAST_API_ENDPOINT = "http://localhost:8080/connector"
OPENPODCAST_API_TOKEN = os.environ.get("OPENPODCAST_API_TOKEN")

# Store data locally for debugging. If this is set to `False`, 
# data will only be sent to Open Podcast API.
# Load from environment variable if set, otherwise default to 0
STORE_DATA = os.environ.get("STORE_DATA", 0) == 1

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
        return requests.post(self.endpoint, headers=headers, json=json)


def fetch_and_capture(
    endpoint_name,
    file_path_prefix,
    connector_call,
    open_podcast_client,
    start,
    end,
    extra_meta={},
):
    """
    Wrapper function to fetch data from Apple and directly send to Open Podcast API.
    """
    logger.info(f"Fetching {endpoint_name}")
    try:
        data = connector_call()
    except Exception as e:
        logger.error(f"Failed to fetch data from {endpoint_name} endpoint: {e}")
        # Silently ignore errors because for some endpoints we don't have data (e.g. `performance`)
        return

    if STORE_DATA:
        with open(f"{file_path_prefix}{dt.datetime.now()}.json", "w+") as f:
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


def main():
    apple_connector = AppleConnector(
        podcast_id=PODCAST_ID,
        myacinfo=MYACINFO,
        itctx=ITCTX,
    )
    open_podcast_client = OpenPodcastApi(
        endpoint=OPENPODCAST_API_ENDPOINT,
        token=OPENPODCAST_API_TOKEN,
    )

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
        "data/podcast/trends/",
        lambda: apple_connector.trends(start, end),
        open_podcast_client,
        start,
        end,
    )

    # Fetch podcast episodes
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

    # Response:
    # "content": {
    #   "results": {
    #       "1000584606852": {
    #           "id": "10005846068
    #            ...
    #       },
    #       ...
    #   }
    # }

    # Iterate over episodes and fetch performance data
    for episode_id, episode in episodes["content"]["results"].items():
        end = dt.datetime.now()
        start = dt.datetime.now() - dt.timedelta(days=7)
        fetch_and_capture(
            "episodeDetails",
            "data/podcast/episodes/",
            lambda: apple_connector.episode(episode_id),
            open_podcast_client,
            start,
            end,
        )

if __name__ == "__main__":
    main()
