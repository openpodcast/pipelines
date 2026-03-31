"""
Anchor / Spotify GraphQL pipeline – main entry point.

Fetches show-level and episode-level analytics from the new Spotify Creators
GraphQL API via ``spotifygraphqlconnector`` and posts the data to the Open
Podcast API in the legacy Anchor-compatible data shapes expected by the backend.
"""

import datetime as dt
import os
import threading
from queue import Queue

from loguru import logger
from spotifygraphqlconnector import SpotifyGraphQLConnector

from job.fetch_params import FetchParams
from job.load_env import load_env, load_file_or_env
from job.open_podcast import OpenPodcastConnector
from job.transforms import (
    transform_aggregated_performance,
    transform_audience_size,
    transform_episode_performance,
    transform_episode_plays,
    transform_episodes_page,
    transform_plays,
    transform_plays_by_age_range,
    transform_plays_by_app,
    transform_plays_by_device,
    transform_plays_by_gender,
    transform_plays_by_geo,
    transform_plays_by_geo_city,
    transform_total_plays,
    transform_total_plays_by_episode,
    transform_unique_listeners,
    wrap_episode_metadata,
)
from job.worker import worker

# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------

print("Initializing environment")

OPENPODCAST_API_ENDPOINT = os.environ.get(
    "OPENPODCAST_API_ENDPOINT", "https://api.openpodcast.dev"
)
OPENPODCAST_API_TOKEN = load_file_or_env("OPENPODCAST_API_TOKEN")

# Spotify Creators GraphQL authentication cookies
SPOTIFY_SP_DC = load_file_or_env("SPOTIFY_SP_DC")
SPOTIFY_SP_KEY = load_file_or_env("SPOTIFY_SP_KEY")

# Optional: Spotify show URI (e.g. "spotify:show:abc123")
# If not set, the connector resolves it automatically from the account.
SPOTIFY_SHOW_URI = load_file_or_env("SPOTIFY_SHOW_URI", "")

# Optional: legacy station ID – only needed if get_all_episodes is called
# without a show URI.  Can also be supplied as PODCAST_ID.
SPOTIFY_STATION_ID = load_file_or_env("SPOTIFY_STATION_ID", "")
if not SPOTIFY_STATION_ID:
    SPOTIFY_STATION_ID = load_file_or_env("PODCAST_ID", "")

# Date range window used for most analytics queries
DATE_RANGE_WINDOW = load_env("DATE_RANGE_WINDOW", "WINDOW_LAST_SEVEN_DAYS")

# Number of worker threads
NUM_WORKERS = int(os.environ.get("NUM_WORKERS", "1"))

# Map window enum → number of lookback days so the API envelope dates
# match the actual data returned by the GraphQL API.
_WINDOW_DAYS = {
    "WINDOW_LAST_SEVEN_DAYS": 7,
    "WINDOW_LAST_THIRTY_DAYS": 30,
    "WINDOW_LAST_NINETY_DAYS": 90,
    "WINDOW_ALL_TIME": 365,  # approximation for envelope only
}
_lookback = _WINDOW_DAYS.get(DATE_RANGE_WINDOW, 30)

START_DATE = (dt.datetime.now() - dt.timedelta(days=_lookback)).date()
END_DATE = (dt.datetime.now() - dt.timedelta(days=1)).date()

# Check required env vars
missing_vars = [
    name
    for name in ("OPENPODCAST_API_TOKEN", "SPOTIFY_SP_DC", "SPOTIFY_SP_KEY")
    if not globals().get(name)
]
if missing_vars:
    logger.error(
        f"Missing required environment variables: {', '.join(missing_vars)}. Exiting..."
    )
    exit(1)

print("Done initializing environment")

# ---------------------------------------------------------------------------
# Connectors
# ---------------------------------------------------------------------------

connector = SpotifyGraphQLConnector(
    sp_dc=SPOTIFY_SP_DC,
    sp_key=SPOTIFY_SP_KEY,
    show_uri=SPOTIFY_SHOW_URI or None,
    station_id=SPOTIFY_STATION_ID or None,
)

# Resolve the show URI once so every subsequent call can reuse it.
show_uri = connector._ensure_show_uri()
logger.info(f"Resolved show URI: {show_uri}")

open_podcast = OpenPodcastConnector(
    OPENPODCAST_API_ENDPOINT,
    OPENPODCAST_API_TOKEN,
    # Use the Spotify show URI as the podcast identifier
    show_uri,
)

# Health check
response = open_podcast.health()
if response.status_code != 200:
    logger.error(
        f"Open Podcast API healthcheck failed with status code {response.status_code}"
    )
    exit(1)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_request_lambda(f, *args, **kwargs):
    """Capture arguments in a closure (call-by-value)."""
    return lambda: f(*args, **kwargs)


# ---------------------------------------------------------------------------
# Pre-fetch shared data (avoids duplicate API calls)
# ---------------------------------------------------------------------------

logger.info("Pre-fetching shared analytics data …")

spotify_stats = connector.get_show_spotify_stats(
    show_uri=show_uri,
    date_range_window=DATE_RANGE_WINDOW,
    include_audience_size=True,
)
platform_stats = connector.get_show_platform_stats(
    show_uri=show_uri,
    date_range_window=DATE_RANGE_WINDOW,
)
demographics_stats = connector.get_show_demographics_stats(
    show_uri=show_uri,
    date_range_window=DATE_RANGE_WINDOW,
)
geo_stats_country = connector.get_show_geo_stats(
    show_uri=show_uri,
    date_range_window=DATE_RANGE_WINDOW,
    result_geo="GEO_COUNTRY",
)
geo_stats_city = connector.get_show_geo_stats(
    show_uri=show_uri,
    date_range_window=DATE_RANGE_WINDOW,
    result_geo="GEO_CITY",
)
discovery_stats = connector.get_show_audience_discovery(
    show_uri=show_uri,
    date_range_window=DATE_RANGE_WINDOW,
)
top_episodes = connector.get_show_top_episodes(show_uri=show_uri)

logger.info("Fetching all episodes …")
raw_episodes = connector.get_all_episodes()

# Build enrichment lookup so transforms can access episodeId, duration, etc.
episode_enrichment = {ep.get("uri", ""): ep for ep in raw_episodes}

logger.info(f"Pre-fetch complete ({len(raw_episodes)} episodes).")

# ---------------------------------------------------------------------------
# Show-level endpoints
# ---------------------------------------------------------------------------

endpoints: list[FetchParams] = [
    FetchParams(
        openpodcast_endpoint="plays",
        anchor_call=lambda: transform_plays(spotify_stats),
        start_date=START_DATE,
        end_date=END_DATE,
    ),
    FetchParams(
        openpodcast_endpoint="playsByApp",
        anchor_call=lambda: transform_plays_by_app(platform_stats),
        start_date=START_DATE,
        end_date=END_DATE,
    ),
    FetchParams(
        openpodcast_endpoint="playsByDevice",
        anchor_call=lambda: transform_plays_by_device(platform_stats),
        start_date=START_DATE,
        end_date=END_DATE,
    ),
    FetchParams(
        openpodcast_endpoint="playsByGeo",
        anchor_call=lambda: transform_plays_by_geo(geo_stats_country),
        start_date=START_DATE,
        end_date=END_DATE,
    ),
    FetchParams(
        openpodcast_endpoint="playsByGeoCity",
        anchor_call=lambda: transform_plays_by_geo_city(geo_stats_city),
        start_date=START_DATE,
        end_date=END_DATE,
    ),
    FetchParams(
        openpodcast_endpoint="playsByAgeRange",
        anchor_call=lambda: transform_plays_by_age_range(demographics_stats),
        start_date=START_DATE,
        end_date=END_DATE,
    ),
    FetchParams(
        openpodcast_endpoint="playsByGender",
        anchor_call=lambda: transform_plays_by_gender(demographics_stats),
        start_date=START_DATE,
        end_date=END_DATE,
    ),
    FetchParams(
        openpodcast_endpoint="uniqueListeners",
        anchor_call=lambda: transform_unique_listeners(discovery_stats),
        start_date=START_DATE,
        end_date=END_DATE,
    ),
    FetchParams(
        openpodcast_endpoint="audienceSize",
        anchor_call=lambda: transform_audience_size(discovery_stats),
        start_date=START_DATE,
        end_date=END_DATE,
    ),
    FetchParams(
        openpodcast_endpoint="totalPlays",
        anchor_call=lambda: transform_total_plays(spotify_stats),
        start_date=START_DATE,
        end_date=END_DATE,
    ),
    FetchParams(
        openpodcast_endpoint="totalPlaysByEpisode",
        anchor_call=lambda: transform_total_plays_by_episode(
            top_episodes, episode_enrichment=episode_enrichment
        ),
        start_date=START_DATE,
        end_date=END_DATE,
    ),
]

# ---------------------------------------------------------------------------
# Episodes
# ---------------------------------------------------------------------------

all_episodes = transform_episodes_page(raw_episodes)

logger.info(f"Sending episodesPage data to Open Podcast ({len(all_episodes)} episodes)")
open_podcast.post(
    "episodesPage",
    None,
    all_episodes,
    START_DATE,
    END_DATE,
)

# episode_enrichment was already built during pre-fetch above.

# ---------------------------------------------------------------------------
# Per-episode endpoints
# ---------------------------------------------------------------------------

for episode in raw_episodes:
    # Extract the Spotify episode URI directly from the episode dict.
    episode_uri = episode.get("uri", "")
    if not episode_uri:
        logger.warning(f"Skipping episode without URI: {episode}")
        continue

    meta = {"episode": episode_uri}

    endpoints += [
        FetchParams(
            openpodcast_endpoint="episodePlays",
            anchor_call=get_request_lambda(
                lambda uri=episode_uri: transform_episode_plays(
                    connector.get_episode_streams_and_downloads(
                        episode_uri=uri,
                        date_range_window=DATE_RANGE_WINDOW,
                    ),
                    uri,
                ),
            ),
            start_date=START_DATE,
            end_date=END_DATE,
            meta=meta,
        ),
        FetchParams(
            openpodcast_endpoint="episodePerformance",
            anchor_call=get_request_lambda(
                lambda uri=episode_uri: transform_episode_performance(
                    connector.get_episode_performance_all_time(episode_uri=uri),
                    uri,
                ),
            ),
            start_date=START_DATE,
            end_date=END_DATE,
            meta=meta,
        ),
        FetchParams(
            openpodcast_endpoint="aggregatedPerformance",
            anchor_call=get_request_lambda(
                lambda uri=episode_uri: transform_aggregated_performance(
                    connector.get_episode_performance_all_time(episode_uri=uri),
                    uri,
                ),
            ),
            start_date=START_DATE,
            end_date=END_DATE,
            meta=meta,
        ),
        FetchParams(
            openpodcast_endpoint="podcastEpisode",
            anchor_call=get_request_lambda(
                lambda uri=episode_uri: wrap_episode_metadata(
                    connector.get_episode_metadata_for_analytics(episode_uri=uri),
                    uri,
                    episode_enrichment=episode_enrichment,
                ),
            ),
            start_date=START_DATE,
            end_date=END_DATE,
            meta=meta,
        ),
    ]

# ---------------------------------------------------------------------------
# Execute via worker queue
# ---------------------------------------------------------------------------

queue: Queue = Queue()

for i in range(NUM_WORKERS):
    t = threading.Thread(target=worker, args=(queue, open_podcast))
    t.daemon = True
    t.start()

for endpoint in endpoints:
    queue.put(endpoint)

queue.join()

print("All items processed.")
