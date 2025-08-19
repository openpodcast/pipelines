import threading
import os
import datetime as dt
import requests

from queue import Queue
from datetime import datetime, timedelta

from job.fetch_params import FetchParams
from job.worker import worker
from job.open_podcast import OpenPodcastConnector
from job.load_env import load_file_or_env
from job.load_env import load_env
from job.dates import get_date_range

from loguru import logger
from anchorconnector import AnchorConnector

print("Initializing environment")

OPENPODCAST_API_ENDPOINT = os.environ.get(
    "OPENPODCAST_API_ENDPOINT", "https://api.openpodcast.dev"
)
OPENPODCAST_API_TOKEN = load_file_or_env("OPENPODCAST_API_TOKEN")

# Note: Anchor API uses Spotify as base URL because it was acquired by Spotify.
# This is not a typo.
BASE_URL = load_file_or_env(
    "ANCHOR_BASE_URL", "https://podcasters.spotify.com/pod/api/proxy/v3"
)

BASE_GRAPHQL_URL = os.environ.get("ANCHOR_BASE_GRAPHQL_URL", "https://creators.spotify.com/pod/graphql")

# Anchor webstation ID which represents the podcast, which we fetch data for
ANCHOR_WEBSTATION_ID = load_file_or_env("ANCHOR_WEBSTATION_ID")

# if ANCHOR_WEBSTATION_ID is not set, try to use PODCAST_ID instead
# this is used by the connector manager to be more generic
if not ANCHOR_WEBSTATION_ID:
    ANCHOR_WEBSTATION_ID = load_file_or_env("PODCAST_ID")

# Anchor cookies needed to authenticate
ANCHOR_PW_S = load_file_or_env("ANCHOR_PW_S")

# Number of worker threads to fetch data from the Anchor API by default
NUM_WORKERS = os.environ.get("NUM_WORKERS", 1)

# Start- and end-date for the data we want to fetch
# Load from environment variable if set, otherwise set to defaults
START_DATE = load_env(
    "START_DATE", (dt.datetime.now() - dt.timedelta(days=30)).strftime("%Y-%m-%d")
)
END_DATE = load_env(
    "END_DATE", (dt.datetime.now() - dt.timedelta(days=1)).strftime("%Y-%m-%d")
)

date_range = get_date_range(START_DATE, END_DATE)

# check if all required environment variables are set
missing_vars = list(
    filter(
        lambda x: globals()[x] is None,
        ["OPENPODCAST_API_TOKEN", "ANCHOR_WEBSTATION_ID", "ANCHOR_PW_S"],
    )
)

if len(missing_vars):
    logger.error(
        f"Missing required environment variables:  {', '.join(missing_vars)}. Exiting..."
    )
    exit(1)

print("Done initializing environment")

anchor = AnchorConnector(
    base_url=BASE_URL,
    base_graphql_url="http://example.com",
    webstation_id=ANCHOR_WEBSTATION_ID,
    anchorpw_s=ANCHOR_PW_S,
)

open_podcast = OpenPodcastConnector(
    OPENPODCAST_API_ENDPOINT,
    OPENPODCAST_API_TOKEN,
    # The webstation ID is used to identify the podcast
    ANCHOR_WEBSTATION_ID,
)

# Check that the Open Podcast API is healthy
response = open_podcast.health()
if response.status_code != 200:
    logger.error(
        f"Open Podcast API healthcheck failed with status code {response.status_code}"
    )
    exit(1)


def get_request_lambda(f, *args, **kwargs):
    """
    Capture arguments in the closure so we can use them later in the call
    to ensure call by value and not call by reference.
    """
    return lambda: f(*args, **kwargs)


def episode_all_time_video_data(connector, web_episode_id):
    """
    Special endpoint for fetching video data for an episode because it can return a 404.
    """
    try:
        return anchor.episode_all_time_video_data(web_episode_id)
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            # Handle the case when the episode has no video data or the URL is incorrect
            logger.info("Episode has no video data or URL is incorrect")
            return None
        else:
            # Re-raise the exception if it's not a 404 error
            raise


endpoints = [
    FetchParams(
        openpodcast_endpoint="plays",
        anchor_call=get_request_lambda(anchor.plays, date_range.start, date_range.end),
        start_date=date_range.start,
        end_date=date_range.end,
    ),
    FetchParams(
        openpodcast_endpoint="playsByAgeRange",
        anchor_call=get_request_lambda(
            anchor.plays_by_age_range, date_range.start, date_range.end
        ),
        start_date=date_range.start,
        end_date=date_range.end,
    ),
    FetchParams(
        openpodcast_endpoint="playsByApp",
        anchor_call=get_request_lambda(
            anchor.plays_by_app, date_range.start, date_range.end
        ),
        start_date=date_range.start,
        end_date=date_range.end,
    ),
    FetchParams(
        openpodcast_endpoint="playsByDevice",
        anchor_call=get_request_lambda(
            anchor.plays_by_device, date_range.start, date_range.end
        ),
        start_date=date_range.start,
        end_date=date_range.end,
    ),
    FetchParams(
        openpodcast_endpoint="playsByGender",
        anchor_call=get_request_lambda(
            anchor.plays_by_gender, date_range.start, date_range.end
        ),
        start_date=date_range.start,
        end_date=date_range.end,
    ),
    FetchParams(
        openpodcast_endpoint="playsByGeo",
        anchor_call=get_request_lambda(anchor.plays_by_geo),
        start_date=date_range.start,
        end_date=date_range.end,
    ),
    FetchParams(
        openpodcast_endpoint="uniqueListeners",
        anchor_call=anchor.unique_listeners,
        start_date=date_range.start,
        end_date=date_range.end,
    ),
    FetchParams(
        openpodcast_endpoint="audienceSize",
        anchor_call=anchor.audience_size,
        start_date=date_range.start,
        end_date=date_range.end,
    ),
    FetchParams(
        openpodcast_endpoint="impressions",
        anchor_call=get_request_lambda(
            anchor.impressions,
            date_range.end
        ),
        # Anchor always returns a fixed range of 30 days
        # To store the correct date range in the DB, we use a fixed date range
        # for the API.
        start_date=(datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"),
        end_date=date_range.end,
    ),
    FetchParams(
        openpodcast_endpoint="totalPlaysByEpisode",
        anchor_call=anchor.total_plays_by_episode,
        start_date=date_range.start,
        end_date=date_range.end,
    ),
    FetchParams(
        openpodcast_endpoint="totalPlays",
        anchor_call=get_request_lambda(anchor.total_plays, True),
        start_date=date_range.start,
        end_date=date_range.end,
    ),
]

# Fetch geo city data
#
# First get the list of all countries
# then get the list of all cities in each country
# (The geo data does not have a date range)
countries = anchor.plays_by_geo()

# Iterate over the list of countries
for row in countries["data"]["rows"]:
    # The country name is in the first column of each row
    country = row[0]

    # Add the endpoint to the list of endpoints
    endpoints += [
        FetchParams(
            openpodcast_endpoint="playsByGeoCity",
            anchor_call=get_request_lambda(anchor.plays_by_geo_city, country),
            start_date=date_range.start,
            end_date=date_range.end,
            meta={"country": country},
        )
    ]

episodes = anchor.episodes()

# We already store episode metadata from the `podcast_episode()` method above,
# but we get additional data from the `episodes()` method (e.g. the mapping
# between `episodeId` and `webEpisodeId`)
all_episodes = list(episodes)
logger.info(f"Sending episodesPage data to Open Podcast")
open_podcast.post(
    "episodesPage",
    None,
    all_episodes,
    date_range.start,
    date_range.end,
)


def wrap_episode_metadata(data):
    """
    Transform the episode data to match the expected format

    This is a special case compared to the other endpoints because the
    episode metadata is returned as a list of episodes, but we only want to
    send one episode at a time to the Open Podcast API.
    The reason is that we use a new Anchor/Spotify API endpoint to fetch the
    individual episodes, which only returns one episode at a time.
    """

    # This is the inner podcast data, which we have to wrap in a JSON object
    transformed_episode = {
        "adCount": 0,
        "created": data.get("created"),
        "createdUnixTimestamp": data.get("createdUnixTimestamp"),
        "description": data.get("description"),
        "duration": data.get("totalDuration"),
        "hourOffset": data.get("hourOffset"),
        "isDeleted": data.get("isDeleted", False),
        "isPublished": data.get("isPublished", False),
        "podcastEpisodeId": data.get("webEpisodeId"),
        "publishOn": data.get("publishOn"),
        "publishOnUnixTimestamp": data.get("publishOnUnixTimestamp", 0)
        * 1000,  # Convert to milliseconds
        "title": data.get("title"),
        "url": (
            data.get("episodeAudios", [{}])[0].get("url")
            if data.get("episodeAudios")
            else None
        ),
        "trackedUrl": data.get("spotifyUrl"),
        "episodeImage": data.get("episodeImage"),
        "shareLinkPath": data.get("shareLinkPath"),
        "shareLinkEmbedPath": data.get("shareLinkEmbedPath"),
    }

    return {
        "allEpisodeWebIds": [data.get("webEpisodeId")],
        "podcastId": data.get("webStationId"),
        "podcastEpisodes": [transformed_episode],
        "totalPodcastEpisodes": 1,
        "vanitySlug": "dummy",
        "stationCreatedDate": "dummy",
    }


for episode in all_episodes:
    # Note: Anchor has two IDs for each episode, the `episodeId` and the
    # `webEpisodeId` We use the `webEpisodeId` to identify the episode because
    # it gets used in the URL of the API endpoints.
    # The `episodeId` is just returned in `totalPlaysByEpisode` and
    # `episodesPage` endpoints.
    web_episode_id = episode["webEpisodeId"]

    # To ensure backwards compatibility,
    # we include the raw ids in the meta data.
    meta = {
        "episode": web_episode_id,
        "episodeIdNum": episode["episodeId"],
        "webEpisodeId": web_episode_id,
    }

    endpoints += [
        FetchParams(
            openpodcast_endpoint="episodePlays",
            anchor_call=get_request_lambda(
                anchor.episode_plays,
                web_episode_id,
                date_range.start,
                date_range.end,
                "daily",
            ),
            start_date=date_range.start,
            end_date=date_range.end,
            meta=meta,
        ),
        FetchParams(
            openpodcast_endpoint="episodePerformance",
            anchor_call=get_request_lambda(anchor.episode_performance, web_episode_id),
            start_date=date_range.start,
            end_date=date_range.end,
            meta=meta,
        ),
        FetchParams(
            openpodcast_endpoint="aggregatedPerformance",
            anchor_call=get_request_lambda(
                anchor.episode_aggregated_performance, web_episode_id
            ),
            start_date=date_range.start,
            end_date=date_range.end,
            meta=meta,
        ),
        FetchParams(
            openpodcast_endpoint="podcastEpisode",
            # Note: We pass the `web_episode_id` as a default argument to the
            # lambda function. This creates a new binding for each iteration of
            # the loop, effectively capturing the correct value of
            # web_episode_id for each episode.
            anchor_call=(
                lambda episode_id=web_episode_id: wrap_episode_metadata(
                    get_request_lambda(anchor.episode_metadata, episode_id)()
                )
            ),
            start_date=date_range.start,
            end_date=date_range.end,
            meta=meta,
        ),
        # TODO: This endpoint is not supported by the Open Podcast API yet
        # FetchParams(
        #     openpodcast_endpoint="episodeAllTimeVideoData",
        #     anchor_call=get_request_lambda(
        #         episode_all_time_video_data,
        #         anchor,
        #         web_episode_id,
        #     ),
        #     start_date=date_range.start,
        #     end_date=date_range.end,
        #     meta=meta,
        # ),
    ]

# Create a queue to hold the FetchParams objects
queue = Queue()

# Start a pool of worker threads to process items from the queue
for i in range(NUM_WORKERS):
    t = threading.Thread(target=worker, args=(queue, open_podcast))
    t.daemon = True
    t.start()

# Add all FetchParams objects to the queue
for endpoint in endpoints:
    queue.put(endpoint)

# Wait for all items in the queue to be processed
queue.join()

print("All items processed.")
