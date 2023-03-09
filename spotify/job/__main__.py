import threading
import os
import datetime as dt
from queue import Queue

from job.fetch_params import FetchParams
from job.worker import worker
from job.open_podcast import OpenPodcastConnector
from job.load_env import load_file_or_env
from job.dates import get_date_range
from job.spotify import get_episode_date_range

from loguru import logger
from spotifyconnector import SpotifyConnector

print("Initializing environment")

BASE_URL = load_file_or_env(
    "SPOTIFY_BASE_URL", "https://generic.wg.spotify.com/podcasters/v0"
)

# Spotify client ID which represents the app (in our case the podcasters app)
SPOTIFY_CLIENT_ID = load_file_or_env(
    "SPOTIFY_CLIENT_ID", "05a1371ee5194c27860b3ff3ff3979d2"
)

# Spotify cookies needed to authenticate
SP_DC = load_file_or_env("SPOTIFY_SP_DC")
SP_KEY = load_file_or_env("SPOTIFY_SP_KEY")

# ID of the podcast we want to fetch data for
SPOTIFY_PODCAST_ID = load_file_or_env("SPOTIFY_PODCAST_ID")

OPENPODCAST_API_ENDPOINT = os.environ.get(
    "OPENPODCAST_API_ENDPOINT", "https://api.openpodcast.dev"
)
OPENPODCAST_API_TOKEN = load_file_or_env("OPENPODCAST_API_TOKEN")

# ID of the podcast we want to fetch data for
SPOTIFY_PODCAST_ID = load_file_or_env("SPOTIFY_PODCAST_ID")

# Store data locally for debugging. If this is set to `False`,
# data will only be sent to Open Podcast API.
# Load from environment variable if set, otherwise default to 0
STORE_DATA = os.environ.get("STORE_DATA", "False").lower() in ("true", "1", "t")


# Number of worker threads to fetch data from the Spotify API by default
NUM_WORKERS = os.environ.get("NUM_WORKERS", 1)

# api has a rate limit of around 20req/30sec.
# using 1.5 seems to lead to almost no rate limit errors
TASK_DELAY = os.environ.get("TASK_DELAY", 1.5)

# Start- and end-date for the data we want to fetch
# Load from environment variable if set, otherwise set to defaults
START_DATE = os.environ.get(
    "START_DATE", (dt.datetime.now() - dt.timedelta(days=4)).strftime("%Y-%m-%d")
)
END_DATE = os.environ.get(
    "END_DATE", (dt.datetime.now() - dt.timedelta(days=1)).strftime("%Y-%m-%d")
)

date_range = get_date_range(START_DATE, END_DATE)

print("Done initializing environment")

spotify = SpotifyConnector(
    base_url=BASE_URL,
    client_id=SPOTIFY_CLIENT_ID,
    podcast_id=SPOTIFY_PODCAST_ID,
    sp_dc=SP_DC,
    sp_key=SP_KEY,
)

open_podcast = OpenPodcastConnector(
    OPENPODCAST_API_ENDPOINT,
    OPENPODCAST_API_TOKEN,
    SPOTIFY_PODCAST_ID,
)

# Check that the Open Podcast API is healthy
response = open_podcast.health()
if response.status_code != 200:
    logger.error(
        f"Open Podcast API healthcheck failed with status code {response.status_code}"
    )
    exit(1)

# Define a list of FetchParams objects with the parameters for each API call
endpoints = [
    FetchParams(
        openpodcast_endpoint="metadata",
        spotify_call=lambda: spotify.metadata(),
        start_date=date_range.start,
        end_date=date_range.end,
    ),
    FetchParams(
        openpodcast_endpoint="listeners",
        spotify_call=lambda: spotify.listeners(date_range.start, date_range.end),
        start_date=date_range.start,
        end_date=date_range.end,
    ),
    FetchParams(
        openpodcast_endpoint="detailedStreams",
        spotify_call=lambda: spotify.streams(date_range.start, date_range.end),
        start_date=date_range.start,
        end_date=date_range.end,
    ),
    FetchParams(
        openpodcast_endpoint="followers",
        spotify_call=lambda: spotify.followers(date_range.start, date_range.end),
        start_date=date_range.start,
        end_date=date_range.end,
    ),
    FetchParams(
        openpodcast_endpoint="episodes",
        spotify_call=lambda: spotify.episodes(date_range.start, date_range.end),
        start_date=dt.datetime(2015, 5, 1),
        end_date=dt.datetime.now(),
    ),
] + [
    # Fetch aggregate data for the podcast for each individual day
    # Otherwise we get all data merged into one
    FetchParams(
        openpodcast_endpoint="aggregate",
        spotify_call=lambda: spotify.aggregate(
            start_date,
            start_date + dt.timedelta(days=1),
        ),
        start_date=start_date,
        end_date=start_date + dt.timedelta(days=1),
    )
    for start_date in date_range
]

# Fetch all episodes. Use a longer time range to make sure we get all episodes
# Convert to list to avoid making multiple API calls as we iterate over the generator
episodes = spotify.episodes(dt.datetime(2015, 5, 1), dt.datetime.now())

for episode in episodes:
    episode_id = episode["id"]

    # Fetch data for each episode
    endpoints += [
        FetchParams(
            openpodcast_endpoint="detailedStreams",
            spotify_call=lambda: spotify.streams(
                date_range.start, date_range.end, episode=episode_id
            ),
            start_date=date_range.start,
            end_date=date_range.end,
            meta={"episode": episode_id},
        ),
        FetchParams(
            openpodcast_endpoint="listeners",
            spotify_call=lambda: spotify.listeners(
                date_range.start, date_range.end, episode=episode_id
            ),
            start_date=date_range.start,
            end_date=date_range.end,
            meta={"episode": episode_id},
        ),
        FetchParams(
            openpodcast_endpoint="performance",
            spotify_call=lambda: spotify.performance(episode=episode_id),
            start_date=date_range.start,
            end_date=date_range.end,
            meta={"episode": episode_id},
        ),
    ]

    # Calculate the date range for the episode to avoid unnecessary API calls
    episode_date_range = get_episode_date_range(episode, date_range)

    endpoints += [
        FetchParams(
            openpodcast_endpoint="aggregate",
            spotify_call=lambda: spotify.aggregate(
                start_date,
                start_date + dt.timedelta(days=1),
                episode=episode_id,
            ),
            start_date=start_date,
            end_date=start_date + dt.timedelta(days=1),
            meta={"episode": episode_id},
        )
        for start_date in episode_date_range
    ]

# Create a queue to hold the FetchParams objects
queue = Queue()

# Start a pool of worker threads to process items from the queue
for i in range(NUM_WORKERS):
    t = threading.Thread(target=worker, args=(queue, open_podcast, TASK_DELAY))
    t.daemon = True
    t.start()

# Add all FetchParams objects to the queue
for endpoint in endpoints:
    queue.put(endpoint)

# Wait for all items in the queue to be processed
queue.join()

print("All items processed.")
