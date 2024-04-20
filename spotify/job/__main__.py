import threading
import os
import sys
import datetime as dt
from queue import Queue

from job.fetch_params import FetchParams
from job.worker import worker
from job.open_podcast import OpenPodcastConnector
from job.load_env import load_file_or_env
from job.load_env import load_env
from job.dates import get_date_range
from job.spotify import get_episode_release_date

from loguru import logger
from spotifyconnector import SpotifyConnector
from spotifyconnector.connector import CredentialsExpired

try:
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

    # if SPOTIFY_PODCAST_ID is not set, try to use PODCAST_ID instead
    # this is used by the connector manager to be more generic
    if not SPOTIFY_PODCAST_ID:
        SPOTIFY_PODCAST_ID = load_file_or_env("PODCAST_ID")

    # Store data locally for debugging. If this is set to `False`,
    # data will only be sent to Open Podcast API.
    # Load from environment variable if set, otherwise default to 0
    STORE_DATA = os.environ.get(
        "STORE_DATA", "False").lower() in ("true", "1", "t")


    # Number of worker threads to fetch data from the Spotify API by default
    NUM_WORKERS = os.environ.get("NUM_WORKERS", 1)

    # api has a rate limit of around 20req/30sec.
    # using 1.5 seems to lead to almost no rate limit errors
    TASK_DELAY = os.environ.get("TASK_DELAY", 1.5)

    # Start- and end-date for the data we want to fetch
    # Load from environment variable if set, otherwise set to defaults
    START_DATE = load_env(
        "START_DATE", (dt.datetime.now() - dt.timedelta(days=4)
                    ).strftime("%Y-%m-%d")
    )
    END_DATE = load_env(
        "END_DATE", (dt.datetime.now() - dt.timedelta(days=1)).strftime("%Y-%m-%d")
    )

    date_range = get_date_range(START_DATE, END_DATE)

    # check if all needed environment variables are set
    missing_vars = list(filter(lambda x: globals()[x] is None,
                        ["SP_DC", "SP_KEY", "SPOTIFY_PODCAST_ID", "OPENPODCAST_API_TOKEN"]))

    if len(missing_vars):
        logger.error(
            f"Missing required environment variables:  {', '.join(missing_vars)}. Exiting...")
        exit(1)

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


    def get_request_lambda(f, *args, **kwargs):
        """
        Capture arguments in the closure so we can use them later in the call
        to ensure call by value and not call by reference.
        """
        return lambda: f(*args, **kwargs)


    todayDate = dt.datetime.now()
    yesterdayDate = todayDate - dt.timedelta(days=1)
    oldestDate = dt.datetime(2015, 5, 1)

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
            spotify_call=get_request_lambda(
                spotify.listeners, date_range.start, date_range.end
            ),
            start_date=date_range.start,
            end_date=date_range.end,
        ),
        FetchParams(
            openpodcast_endpoint="detailedStreams",
            spotify_call=get_request_lambda(
                spotify.streams, date_range.start, date_range.end
            ),
            start_date=date_range.start,
            end_date=date_range.end,
        ),
        FetchParams(
            openpodcast_endpoint="followers",
            spotify_call=get_request_lambda(
                spotify.followers, date_range.start, date_range.end
            ),
            start_date=date_range.start,
            end_date=date_range.end,
        ),
        FetchParams(
            openpodcast_endpoint="episodes",
            spotify_call=get_request_lambda(
                # as Spotify sometimes returns empty values if the end date is set to today,
                # we set the end date to yesterday just to be on the safe side
                # see issue https://github.com/openpodcast/api/issues/133
                spotify.episodes, oldestDate, yesterdayDate
            ),
            start_date=oldestDate,
            end_date=todayDate,
        ),
    ] + [
        # Fetch aggregate data for the podcast for each individual day
        # Otherwise we get all data merged into one
        FetchParams(
            openpodcast_endpoint="aggregate",
            spotify_call=get_request_lambda(
                spotify.aggregate, current_date, current_date),
            start_date=current_date,
            end_date=current_date,
        )
        for current_date in date_range
    ]

    # Fetch all episodes. Use a longer time range to make sure we get all episodes
    # Convert to list to avoid making multiple API calls as we iterate over the generator
    episodes = spotify.episodes(oldestDate, todayDate)

    for episode in episodes:
        episode_id = episode["id"]

        # Fetch data for each episode
        endpoints += [
            FetchParams(
                openpodcast_endpoint="detailedStreams",
                spotify_call=get_request_lambda(
                    spotify.streams, date_range.start, date_range.end, episode=episode_id
                ),
                start_date=date_range.start,
                end_date=date_range.end,
                meta={"episode": episode_id},
            ),
            FetchParams(
                openpodcast_endpoint="listeners",
                spotify_call=get_request_lambda(
                    spotify.listeners, date_range.start, date_range.end, episode=episode_id
                ),
                start_date=date_range.start,
                end_date=date_range.end,
                meta={"episode": episode_id},
            ),
            FetchParams(
                openpodcast_endpoint="performance",
                spotify_call=get_request_lambda(
                    spotify.performance, episode=episode_id),
                start_date=date_range.start,
                end_date=date_range.end,
                meta={"episode": episode_id},
            ),
        ]

        # Calculate the date range for the episode to avoid unnecessary API calls
        release_date = get_episode_release_date(episode)
        if not release_date:
            release_date = date_range

        # Start at the release date of the episode or the start date of the time
        # range, whichever is later
        episode_start_date = max(release_date, date_range.start)
        episode_end_date = date_range.end

        # if the end date is smaller than the start date, the episode was just released
        # and we don't have any data for it yet, so we skip it
        if episode_end_date < episode_start_date:
            continue

        episode_date_range = get_date_range(
            episode_start_date.strftime("%Y-%m-%d"),
            episode_end_date.strftime("%Y-%m-%d")
        )

        endpoints += [
            FetchParams(
                openpodcast_endpoint="aggregate",
                spotify_call=get_request_lambda(
                    spotify.aggregate,
                    current_date,
                    current_date,
                    episode=episode_id,
                ),
                start_date=current_date,
                end_date=current_date,
                meta={"episode": episode_id},
            )
            for current_date in episode_date_range
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

except CredentialsExpired as e:
    # Cleanly handle expired credential cookie
    logger.error(f"Authentication failed: {e}")
    sys.exit(1)  
