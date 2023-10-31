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
from podigeeconnector import PodigeeConnector

print("Initializing environment")

OPENPODCAST_API_ENDPOINT = os.environ.get(
    "OPENPODCAST_API_ENDPOINT", "https://api.openpodcast.dev"
)
OPENPODCAST_API_TOKEN = load_file_or_env("OPENPODCAST_API_TOKEN")

# Note: Podigee API uses Spotify as base URL because it was acquired by Spotify.
# This is not a typo.
BASE_URL = load_file_or_env(
    "PODIGEE_BASE_URL", "https://app.podigee.com/api/v1"
)

# Podigee webstation ID which represents the podcast, which we fetch data for
PODIGEE_PODCAST_ID = load_file_or_env("PODIGEE_PODCAST_ID")

# if PODIGEE_PODCAST_ID is not set, try to use PODCAST_ID instead
# this is used by the connector manager to be more generic
if not PODIGEE_PODCAST_ID:
    PODIGEE_PODCAST_ID = load_file_or_env("PODCAST_ID")

# Podigee authentication
PODIGEE_USERNAME = load_file_or_env("PODIGEE_USERNAME")
PODIGEE_PASSWORD = load_file_or_env("PODIGEE_PASSWORD")

# Number of worker threads to fetch data from the Podigee API by default
NUM_WORKERS = os.environ.get("NUM_WORKERS", 1)

# Start- and end-date for the data we want to fetch
# Load from environment variable if set, otherwise set to defaults
START_DATE = load_env(
    "START_DATE", (dt.datetime.now() - dt.timedelta(days=30)
                   ).strftime("%Y-%m-%d")
)
END_DATE = load_env(
    "END_DATE", (dt.datetime.now() - dt.timedelta(days=1)).strftime("%Y-%m-%d")
)

date_range = get_date_range(START_DATE, END_DATE)

# check if all required environment variables are set
missing_vars = list(
    filter(
        lambda x: globals()[x] is None,
        ["OPENPODCAST_API_TOKEN", "PODIGEE_PODCAST_ID", "PODIGEE_USERNAME", "PODIGEE_PASSWORD"],
    )
)

if len(missing_vars):
    logger.error(
        f"Missing required environment variables:  {', '.join(missing_vars)}. Exiting..."
    )
    exit(1)

print("Done initializing environment")

podigee = PodigeeConnector.from_credentials(
    base_url=BASE_URL,
    podcast_id=PODCAST_ID,
    username=PODIGEE_USERNAME,
    password=PODIGEE_PASSWORD,
)

open_podcast = OpenPodcastConnector(
    OPENPODCAST_API_ENDPOINT,
    OPENPODCAST_API_TOKEN,
    # The podcast ID is used to identify the podcast
    PODIGEE_PODCAST_ID,
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


endpoints = [
    FetchParams(
        openpodcast_endpoint="podcastAnalytics",
        podigee_call=lambda: podigee.podcast_analytics(),
        start_date=date_range.start,
        end_date=date_range.end,
    ),
]

episodes = podigee.episodes()

for episode in episodes:
    endpoints += [
        FetchParams(
            openpodcast_endpoint="episodeAnalytics",
            podigee_call=get_request_lambda(
                podigee.episode_analytics, episode.id),
            start_date=date_range.start,
            end_date=date_range.end,
        ),
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
