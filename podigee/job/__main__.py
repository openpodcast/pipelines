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

BASE_URL = load_file_or_env(
    "PODIGEE_BASE_URL", "https://app.podigee.com/api/v1"
)

# Podigee podcast IDs are integers and different from Open Podcast IDs
# One Podigee account can have multiple podcasts, so we need to specify the podcast ID
# of the podcast we want to fetch data for
PODCAST_ID = load_file_or_env("PODCAST_ID")

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
always_required = ["OPENPODCAST_API_TOKEN", "PODCAST_ID"]
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

# The Podigee podcast ID is expected to be an integer
# Try to convert it to an integer, if it fails, this throws exception 
try:
    PODCAST_ID = int(PODCAST_ID)
except ValueError:
    logger.error(f"PODCAST_ID must be an integer, got: {PODCAST_ID}")
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

# # for testing, just output the podcast names and ids and then exit the whole program
# for p in podcasts:
#     logger.info(f"Found podcast: {p['title']} (ID: {p['id']})")
# # exit
# exit(0)

if not podcasts:
    logger.error("No podcasts found")
    exit(1)

# Find the podcast we want to work with
podcast = None
for p in podcasts:
    if p["id"] == PODCAST_ID:
        podcast = p
        break

if not podcast:
    logger.error(
        f"Podcast with ID {PODCAST_ID} not found. Available podcasts: {[p['id'] for p in podcasts]}"
    )
    exit(1)

# Extract and validate podcast title
podcast_title = podcast.get("title")
if not podcast_title:
    logger.error(f"Podcast with ID {PODCAST_ID} has no title")
    exit(1)

open_podcast = OpenPodcastConnector(
    OPENPODCAST_API_ENDPOINT,
    OPENPODCAST_API_TOKEN,
    PODCAST_ID
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


def get_podcast_metadata():
    """
    Get podcast metadata formatted for OpenPodcast API.
    """
    return {
        "name": podcast_title
    }


def transform_podigee_analytics_to_metrics(analytics_data):
    """
    Transform Podigee analytics data to OpenPodcast metrics format.
    Expected format: {"metrics": [{"start": "date", "end": "date", "dimension": "string", "subdimension": "string", "value": number}]}
    """
    if not analytics_data or "objects" not in analytics_data:
        return {"metrics": []}
    
    metrics = []
    
    for day_data in analytics_data["objects"]:
        date = day_data.get("downloaded_on", "").split("T")[0]  # Extract YYYY-MM-DD
        if not date:
            continue
            
        # Process downloads
        if "downloads" in day_data:
            for download_type, value in day_data["downloads"].items():
                metrics.append({
                    "start": date,
                    "end": date,
                    "dimension": "downloads",
                    "subdimension": download_type,
                    "value": value
                })
        
        # Process platforms
        if "platforms" in day_data:
            for platform, value in day_data["platforms"].items():
                metrics.append({
                    "start": date,
                    "end": date,
                    "dimension": "platforms",
                    "subdimension": platform,
                    "value": value
                })
        
        # Process clients
        if "clients" in day_data:
            for client, value in day_data["clients"].items():
                metrics.append({
                    "start": date,
                    "end": date,
                    "dimension": "clients",
                    "subdimension": client,
                    "value": value
                })
        
        # Process sources
        if "sources" in day_data:
            for source, value in day_data["sources"].items():
                metrics.append({
                    "start": date,
                    "end": date,
                    "dimension": "sources",
                    "subdimension": source,
                    "value": value
                })
    
    return {"metrics": metrics}


endpoints = [
    # Podcast metadata - get basic podcast information
    FetchParams(
        openpodcast_endpoint="metadata",
        podigee_call=get_podcast_metadata,
        start_date=date_range.start,
        end_date=date_range.end,
    ),
    # Podcast metrics - analytics data for the podcast
    FetchParams(
        openpodcast_endpoint="metrics", 
        podigee_call=lambda: transform_podigee_analytics_to_metrics(
            podigee.podcast_analytics(PODCAST_ID, start=date_range.start, end=date_range.end)
        ),
        start_date=date_range.start,
        end_date=date_range.end,
    ),
]

episodes = podigee.episodes(PODCAST_ID)

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
                lambda ep_id: transform_podigee_analytics_to_metrics(
                    podigee.episode_analytics(ep_id, granularity=None, start=date_range.start, end=date_range.end)
                ), 
                str(episode["id"])
            ),
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
