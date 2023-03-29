import threading
import os
import datetime as dt
from queue import Queue

from job.fetch_params import FetchParams
from job.worker import worker
from job.open_podcast import OpenPodcastConnector
from job.load_env import load_file_or_env
from job.load_env import load_env
from job.dates import get_date_range
import job.apple as apple

from loguru import logger
from appleconnector import AppleConnector, Metric, Dimension

print("Initializing environment")

# endpoint to receive apple cookie to access podcasters API
APPLE_AUTOMATION_ENDPOINT = load_file_or_env("APPLE_AUTOMATION_ENDPOINT")
APPLE_AUTOMATION_BEARER_TOKEN = load_file_or_env("APPLE_AUTOMATION_BEARER_TOKEN")

# ID of the podcast we want to fetch data for
APPLE_PODCAST_ID = os.environ.get("APPLE_PODCAST_ID")

# Open Podcast API endpoint and token to submit data fetched from the spotify endpoint
OPENPODCAST_API_ENDPOINT = os.environ.get(
    "OPENPODCAST_API_ENDPOINT", "https://api.openpodcast.dev"
)
OPENPODCAST_API_TOKEN = load_file_or_env("OPENPODCAST_API_TOKEN")

# Store data locally for debugging. If this is set to `False`,
# data will only be sent to Open Podcast API.
# Load from environment variable if set, otherwise default to 0
STORE_DATA = os.environ.get("STORE_DATA", "False") == "True"

# Store data locally for debugging. If this is set to `False`,
# data will only be sent to Open Podcast API.
# Load from environment variable if set, otherwise default to 0
STORE_DATA = os.environ.get("STORE_DATA", "False").lower() in ("true", "1", "t")

# Number of worker threads to fetch data from the Spotify API by default
NUM_WORKERS = os.environ.get("NUM_WORKERS", 1)

TASK_DELAY = os.environ.get("TASK_DELAY", 1.5)

# Start- and end-date for the data we want to fetch
# Load from environment variable if set, otherwise set to defaults
START_DATE = load_env(
    "START_DATE", (dt.datetime.now() - dt.timedelta(days=7)).strftime("%Y-%m-%d")
)
END_DATE = load_env("END_DATE", (dt.datetime.now()).strftime("%Y-%m-%d"))

# The trends API supports historical data imports with daily resolution
# up to 4 months in the past.
# If we want to import a longer date-range, we split the date range into chunks
# of 4 months to fetch the data in multiple requests.
# This allows us to fetch the data quicker and avoid hitting the rate limit.
DAYS_PER_CHUNK = os.environ.get("DAYS_PER_CHUNK", 4 * 30)

date_range = get_date_range(START_DATE, END_DATE)

print("Done initializing environment")
print(f"Import date range: ", date_range)


def get_request_lambda(f, *args, **kwargs):
    """
    Capture arguments in the closure so we can use them later in the call
    to ensure call by value and not call by reference.
    """
    return lambda: f(*args, **kwargs)


open_podcast = OpenPodcastConnector(
    OPENPODCAST_API_ENDPOINT,
    OPENPODCAST_API_TOKEN,
    APPLE_PODCAST_ID,
)

# Check that the Open Podcast API is healthy
response = open_podcast.health()
if response.status_code != 200:
    logger.error(
        f"Open Podcast API healthcheck failed with status code {response.status_code}"
    )
    exit(1)

logger.info(
    f"Receiving cookies from Apple from automation endpoint {APPLE_AUTOMATION_ENDPOINT}"
)
cookies = apple.get_cookies(APPLE_AUTOMATION_BEARER_TOKEN, APPLE_AUTOMATION_ENDPOINT)

apple_connector = AppleConnector(
    podcast_id=APPLE_PODCAST_ID,
    myacinfo=cookies.myacinfo,
    itctx=cookies.itctx,
)

# Define a list of FetchParams objects with the parameters for each API call
endpoints = []

for chunk_id, (start_date, end_date) in enumerate(date_range.chunks(DAYS_PER_CHUNK)):
    print(f"Chunk {chunk_id} from {start_date} to {end_date}...")
    endpoints += [
        FetchParams(
            openpodcast_endpoint="showTrends/Followers",
            call=get_request_lambda(
                apple_connector.trends,
                start_date,
                end_date,
                metric=Metric.FOLLOWERS,
            ),
            start_date=start_date,
            end_date=end_date,
            meta={"metric": Metric.FOLLOWERS},
        ),
        FetchParams(
            openpodcast_endpoint="showTrends/Listeners",
            call=get_request_lambda(
                apple_connector.trends,
                start_date,
                end_date,
                metric=Metric.LISTENERS,
                dimension=Dimension.BY_EPISODES,
            ),
            start_date=start_date,
            end_date=end_date,
            meta={
                "metric": Metric.LISTENERS,
                "dimension": Dimension.BY_EPISODES,
            },
        ),
        # fetch podcast listening time grouped by (non)followers
        FetchParams(
            openpodcast_endpoint="showTrends/FollowersListeningTime",
            call=get_request_lambda(
                apple_connector.trends,
                start_date,
                end_date,
                metric=Metric.TIME_LISTENED,
                dimension=Dimension.BY_FOLLOW_STATE,
            ),
            start_date=start_date,
            end_date=end_date,
            meta={
                "metric": Metric.TIME_LISTENED,
                "dimension": Dimension.BY_FOLLOW_STATE,
            },
        ),
        # fetch podcast listening time grouped by episodes
        FetchParams(
            openpodcast_endpoint="showTrends/EpisodesListeningTime",
            call=get_request_lambda(
                apple_connector.trends,
                start_date,
                end_date,
                metric=Metric.TIME_LISTENED,
                dimension=Dimension.BY_EPISODES,
            ),
            start_date=start_date,
            end_date=end_date,
            meta={
                "metric": Metric.TIME_LISTENED,
                "dimension": Dimension.BY_EPISODES,
            },
        )
    ]

endpoints += [
    FetchParams(
        openpodcast_endpoint="episodes",
        call=lambda: apple_connector.episodes(),
        start_date=date_range.start,
        end_date=date_range.end,
    ),
]

# Fetch all episodes to get the episode IDs
# for which we want to fetch data
episodes = apple.get_episode_ids(apple_connector)

for episode_id in episodes:
    endpoints += [
        FetchParams(
            openpodcast_endpoint="episodeDetails",
            call=get_request_lambda(
                apple_connector.episode,
                episode_id,
            ),
            start_date=date_range.start,
            end_date=date_range.end,
            meta={
                "episode": episode_id,
            },
        ),
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
