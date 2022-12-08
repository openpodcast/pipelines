import os
import datetime as dt
import json
import time
from loguru import logger
from spotifyconnector import SpotifyConnector
import requests
import types

BASE_URL = "https://generic.wg.spotify.com/podcasters/v0"
CLIENT_ID = "05a1371ee5194c27860b3ff3ff3979d2"
SPOTIFY_PODCAST_ID = os.environ.get("SPOTIFY_PODCAST_ID")
SP_DC = os.environ.get("SPOTIFY_SP_DC")
SP_KEY = os.environ.get("SPOTIFY_SP_KEY")
FEED_URL = "https://feeds.redcircle.com/2c2cd740-1c1f-4928-adac-98a692dbf4c2"
OPENPODCAST_API_ENDPOINT = os.environ.get("OPENPODCAST_API_ENDPOINT")
OPENPODCAST_API_TOKEN = os.environ.get("OPENPODCAST_API_TOKEN")

# Store data locally for debugging. If this is set to `False`,
# data will only be sent to Open Podcast API.
# Load from environment variable if set, otherwise default to 0
STORE_DATA = os.environ.get("STORE_DATA", "False").lower() in ("true", "1", "t")


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
            "provider": "spotify",
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
    Wrapper function to fetch data from Spotify and directly send to Open Podcast API.
    """
    logger.info(f"Fetching {endpoint_name}")
    try:
        data = connector_call()
    except Exception as e:
        logger.error(f"Failed to fetch data from {endpoint_name} endpoint: {e}")
        # Silently ignore errors because for some endpoints we don't have data (e.g. `performance`)
        return

    # If the data is a generator, we need convert it to a list with
    # `endpoint_name` as the key (e.g. for `episodes` and `detailedStreams`)
    if isinstance(data, types.GeneratorType):
        data = {endpoint_name: list(data)}

    if STORE_DATA:
        with open(f"{file_path_prefix}{dt.datetime.now()}.json", "w+") as f:
            json.dump(data, f)

    result = open_podcast_client.capture(
        data,
        # Merge in extra metadata (if any)
        meta={
            **extra_meta,
            **{
                "show": SPOTIFY_PODCAST_ID,
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
    spotify_connector = SpotifyConnector(
        base_url=BASE_URL,
        client_id=CLIENT_ID,
        podcast_id=SPOTIFY_PODCAST_ID,
        sp_dc=SP_DC,
        sp_key=SP_KEY,
    )
    open_podcast_client = OpenPodcastApi(
        endpoint=OPENPODCAST_API_ENDPOINT,
        token=OPENPODCAST_API_TOKEN,
    )

    # Check if API is up before sending data
    if not api_healthcheck(open_podcast_client):
        logger.error("Open Podcast API is not up. Quitting")
        return

    start = dt.datetime.now() - dt.timedelta(days=1)
    end = dt.datetime.now()
    fetch_and_capture(
        "metadata",
        "data/podcast/metadata/",
        lambda: spotify_connector.metadata(),
        open_podcast_client,
        start,
        end,
    )

    start = dt.datetime.now() - dt.timedelta(days=3)
    end = dt.datetime.now()
    fetch_and_capture(
        "detailedStreams",
        "data/podcast/streams/",
        lambda: spotify_connector.streams(start, end),
        open_podcast_client,
        start,
        end,
    )

    start = dt.datetime.now() - dt.timedelta(days=3)
    end = dt.datetime.now()
    fetch_and_capture(
        "listeners",
        "data/podcast/listeners/",
        lambda: spotify_connector.listeners(start, end),
        open_podcast_client,
        start,
        end,
    )

    for i in range(3):
        end = dt.datetime.now() - dt.timedelta(days=i)
        start = end - dt.timedelta(days=1)
        fetch_and_capture(
            "aggregate",
            "data/podcast/aggregate/",
            lambda: spotify_connector.aggregate(start, end),
            open_podcast_client,
            start,
            end,
        )

    start = dt.datetime.now() - dt.timedelta(days=3)
    end = dt.datetime.now()
    fetch_and_capture(
        "followers",
        "data/podcast/followers/",
        lambda: spotify_connector.followers(start, end),
        open_podcast_client,
        start,
        end,
    )

    # Fetch all episodes. We need to specify a range here because the API
    # requires it, so let's use a long range.
    start = dt.datetime(2015, 5, 1)
    end = dt.datetime.now()
    episodes = fetch_and_capture(
        "episodes",
        "data/podcast/episodes/",
        lambda: spotify_connector.episodes(start, end),
        open_podcast_client,
        start,
        end,
    )

    for episode in episodes["episodes"]:
        id = episode["id"]

        # Do we want to fetch episode metadata? It is supported by the client
        # but we don't use it at the moment.
        # fetch_and_capture("episode_metadata", f"data/episodes/metadata/{id}", lambda: spotify_connector.episode_metadata(id, start, end), open_podcast_client, start, end)

        start = dt.datetime.now() - dt.timedelta(days=3)
        end = dt.datetime.now()
        fetch_and_capture(
            "detailedStreams",
            f"data/episodes/streams/{id}-",
            lambda: spotify_connector.streams(start, end, episode=id),
            open_podcast_client,
            start,
            end,
            extra_meta={
                "episode": id,
            },
        )

        start = dt.datetime.now() - dt.timedelta(days=30)
        end = dt.datetime.now()
        fetch_and_capture(
            "listeners",
            f"data/episodes/listeners/{id}-",
            lambda: spotify_connector.listeners(start, end, episode=id),
            open_podcast_client,
            start,
            end,
            extra_meta={
                "episode": id,
            },
        )

        start = dt.datetime.now() - dt.timedelta(days=30)
        end = dt.datetime.now()
        fetch_and_capture(
            "performance",
            f"data/episodes/performance/{id}-",
            lambda: spotify_connector.performance(id),
            open_podcast_client,
            start,
            end,
            extra_meta={
                "episode": id,
            },
        )

        # Fetch aggregate data for the episode in 3x1 day changes
        # (today, yesterday, the day before yesterday)
        # Otherwise you get aggregated data of 3 days.
        for i in range(3):
            end = dt.datetime.now() - dt.timedelta(days=i)
            start = end - dt.timedelta(days=1)
            fetch_and_capture(
                "aggregate",
                f"data/episodes/aggregate/{id}-",
                lambda: spotify_connector.aggregate(start, end, episode=id),
                open_podcast_client,
                start,
                end,
                extra_meta={
                    "episode": id,
                },
            )


if __name__ == "__main__":
    main()
