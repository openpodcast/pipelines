import threading
import os
import datetime as dt
import requests
import json

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
OPENPODCAST_PODCAST_ID = load_file_or_env("OPENPODCAST_PODCAST_ID")

BASE_URL = load_file_or_env(
    "PODIGEE_BASE_URL", "https://app.podigee.com/api/v1"
)

# Podigee podcast IDs are integers and different from Open Podcast IDs
# One Podigee account can have multiple podcasts, so we need to specify the podcast ID
# of the podcast we want to fetch data for
PODIGEE_PODCAST_ID = load_file_or_env("PODIGEE_PODCAST_ID")

# Podigee authentication
PODIGEE_ACCESS_TOKEN = load_file_or_env("PODIGEE_ACCESS_TOKEN")
PODIGEE_USERNAME = load_file_or_env("PODIGEE_USERNAME")
PODIGEE_PASSWORD = load_file_or_env("PODIGEE_PASSWORD")

# Number of worker threads to fetch data from the Podigee API by default
NUM_WORKERS = int(os.environ.get("NUM_WORKERS", 1))

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
always_required = ["OPENPODCAST_API_TOKEN", "PODIGEE_PODCAST_ID", "OPENPODCAST_PODCAST_ID"]
missing_always_required = list(
    filter(
        lambda x: globals()[x] is None,
        always_required,
    )
)

if len(missing_always_required):
    logger.error(
        f"Missing required environment variables: {', '.join(missing_always_required)}. Exiting..."
    )
    exit(1)

# Check authentication methods - require either API token OR username+password
has_api_token = PODIGEE_ACCESS_TOKEN is not None
has_credentials = PODIGEE_USERNAME is not None and PODIGEE_PASSWORD is not None

if not has_api_token and not has_credentials:
    logger.error(
        "Missing Podigee authentication. Please provide either PODIGEE_ACCESS_TOKEN or both PODIGEE_USERNAME and PODIGEE_PASSWORD. Exiting..."
    )
    exit(1)

# We know that PODIGEE_PODCAST_ID is set and is an integer
# Try to convert it to an integer, if it fails, this throws exception 
try:
    PODIGEE_PODCAST_ID = int(PODIGEE_PODCAST_ID)
except ValueError:
    logger.error(f"PODIGEE_PODCAST_ID must be an integer, got: {PODIGEE_PODCAST_ID}")
    exit(1)

print("Done initializing environment")

# Try API token first (preferred method), fallback to username/password
if has_api_token:
    logger.info("Using Podigee API token for authentication")
    podigee = PodigeeConnector(
        base_url=BASE_URL,
        podigee_access_token=PODIGEE_ACCESS_TOKEN,
    )
else:
    logger.info("Fallback: Using Podigee username/password for authentication. Set API token to use it instead.")
    podigee = PodigeeConnector.from_credentials(
        base_url=BASE_URL,
        username=PODIGEE_USERNAME,
        password=PODIGEE_PASSWORD,
    )

podcasts = podigee.podcasts()
logger.debug("Podcasts = {}", json.dumps(podcasts, indent=4))

if not podcasts:
    logger.error("No podcasts found")
    exit(1)

# Check if the specified podcast exists
if PODIGEE_PODCAST_ID not in [podcast["id"] for podcast in podcasts]:
    logger.error(
        f"Podcast with ID {PODIGEE_PODCAST_ID} not found. Available podcasts: {[podcast['id'] for podcast in podcasts]}"
    )
    exit(1)

open_podcast = OpenPodcastConnector(
    OPENPODCAST_API_ENDPOINT,
    OPENPODCAST_API_TOKEN,
    OPENPODCAST_PODCAST_ID, # The podcast ID is used to identify the podcast (this is the Open Podcast API ID, not the Podigee ID)
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
    # Podcast metadata - get basic podcast information
    FetchParams(
        openpodcast_endpoint="metadata",
        podigee_call=lambda: {
            "name": next((p for p in podigee.podcasts() if p["id"] == PODIGEE_PODCAST_ID), {}).get("title", "")
        },
        start_date=date_range.start,
        end_date=date_range.end,
    ),
    # Podcast metrics - analytics data for the podcast
    FetchParams(
        openpodcast_endpoint="metrics", 
        podigee_call=lambda: podigee.podcast_analytics(PODIGEE_PODCAST_ID, start=date_range.start, end=date_range.end),
        start_date=date_range.start,
        end_date=date_range.end,
    ),
]

episodes = podigee.episodes(PODIGEE_PODCAST_ID)

for episode in episodes:
    print(episode)
    endpoints += [
        # Episode metadata - basic episode information
        FetchParams(
            openpodcast_endpoint="metadata",
            podigee_call=get_request_lambda(lambda ep: {
                "ep_name": ep.get("title", ""),
                "ep_url": ep.get("url", ""),
                "ep_release_date": ep.get("published_at", "")
            }, episode),
            start_date=date_range.start,
            end_date=date_range.end,
            meta={"episode": str(episode["id"])},
        ),
        # Episode metrics - analytics data for the episode
        FetchParams(
            openpodcast_endpoint="metrics",
            podigee_call=get_request_lambda(
                podigee.episode_analytics, str(episode["id"]), granularity=None, start=date_range.start, end=date_range.end),
            start_date=date_range.start,
            end_date=date_range.end,
            meta={"episode": str(episode["id"])},
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
