"""
Anchor / Spotify GraphQL pipeline – main entry point.

Fetches show-level and episode-level analytics from the new Spotify Creators
GraphQL API via ``spotifygraphqlconnector`` and posts the data to the Open
Podcast API in the legacy Anchor-compatible data shapes expected by the backend.
"""

import os
import threading
import datetime as dt
from queue import Queue

from loguru import logger
from spotifygraphqlconnector import SpotifyGraphQLConnector

from job.dates import get_date_range
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
PODCAST_ID = load_file_or_env("PODCAST_ID", "")

if not SPOTIFY_SHOW_URI and PODCAST_ID.startswith("spotify:show:"):
    SPOTIFY_SHOW_URI = PODCAST_ID

# Optional: legacy station ID – only needed if get_all_episodes is called
# without a show URI.  Can also be supplied as PODCAST_ID when numeric.
SPOTIFY_STATION_ID = load_file_or_env("SPOTIFY_STATION_ID", "")
if not SPOTIFY_STATION_ID and PODCAST_ID.isdigit():
    SPOTIFY_STATION_ID = PODCAST_ID

# Date range used for analytics queries.
START_DATE_STR = load_env(
    "START_DATE",
    (dt.datetime.now() - dt.timedelta(days=3)).strftime("%Y-%m-%d"),
)
END_DATE_STR = load_env(
    "END_DATE",
    (dt.datetime.now() - dt.timedelta(days=1)).strftime("%Y-%m-%d"),
)

# Number of worker threads
NUM_WORKERS = int(os.environ.get("NUM_WORKERS", "1"))

date_range = get_date_range(START_DATE_STR, END_DATE_STR)
START_DATE = date_range.start.date()
END_DATE = date_range.end.date()

logger.info(f"Using explicit date range {START_DATE} - {END_DATE}.")

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


def get_top_geo_name(geo_payload: dict) -> str | None:
    """Extract top geo displayName from a geo stats response."""
    geos = (
        geo_payload.get("showByShowUri", {})
        .get("showStreamsAndDownloadsByGeo", {})
        .get("analyticsValue", {})
        .get("analyticsValue", {})
        .get("geos", [])
    )
    if not geos:
        return None
    return geos[0].get("displayName")


def get_numeric_episode_id(episode: dict) -> int | str | None:
    """Return numeric Anchor episode ID from episode payload variants."""
    return episode.get("id") or episode.get("episodeId") or episode.get("stationEpisodeId")


# ---------------------------------------------------------------------------
# Pre-fetch shared data (avoids duplicate API calls)
# ---------------------------------------------------------------------------

logger.info("Pre-fetching shared analytics data …")

spotify_stats = connector.get_show_spotify_stats(
    show_uri=show_uri,
    include_audience_size=True,
    start_date=START_DATE,
    end_date=END_DATE,
)
platform_stats = connector.get_show_platform_stats(
    show_uri=show_uri,
    start_date=START_DATE,
    end_date=END_DATE,
)
demographics_stats = connector.get_show_demographics_stats(
    show_uri=show_uri,
    start_date=START_DATE,
    end_date=END_DATE,
)
geo_stats_country = connector.get_show_geo_stats(
    show_uri=show_uri,
    result_geo="GEO_COUNTRY",
    start_date=START_DATE,
    end_date=END_DATE,
)

# Geo drill-down for top country.
geo_city_country: str | None = None
geo_city_region: str | None = None
geo_stats_city: dict = {}
geo_stats_region: dict = {}

top_country = get_top_geo_name(geo_stats_country)
if top_country:
    geo_city_country = top_country
    try:
        geo_stats_region = connector.get_show_geo_stats(
            show_uri=show_uri,
            result_geo="GEO_REGION",
            country=top_country,
            start_date=START_DATE,
            end_date=END_DATE,
        )
        top_region = get_top_geo_name(geo_stats_region)
        if top_region:
            geo_city_region = top_region
            
        # The newer connector no longer requires passing a specific region to fetch GEO_CITY.
        # Thus, GEO_CITY drill-down is no longer gated behind successfully retrieving a top_region.
        geo_stats_city = connector.get_show_geo_stats(
            show_uri=show_uri,
            result_geo="GEO_CITY",
            country=top_country,
            start_date=START_DATE,
            end_date=END_DATE,
        )
        logger.info(
            f"Fetched GEO_REGION and GEO_CITY drill-down for {top_country}."
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"GEO drill-down fetch failed, keeping empty payloads: {exc}")
else:
    logger.warning("No GEO_COUNTRY data available; cannot fetch GEO_CITY drill-down.")
discovery_stats = connector.get_show_audience_discovery(
    show_uri=show_uri,
    start_date=START_DATE,
    end_date=END_DATE,
)
all_time_show_stats = connector.get_streams_and_downloads_all_time(show_uri=show_uri)

logger.info("Fetching all episodes …")
raw_episodes = connector.get_all_episodes()

logger.info("Fetching all-time plays per episode …")
all_time_episode_plays = []
for ep in raw_episodes:
    ep_uri = ep.get("uri")
    if not ep_uri:
        continue
    try:
        plays_data = connector.get_episode_plays_total(episode_uri=ep_uri)
        all_time_episode_plays.append({
            "uri": ep_uri,
            "episode": ep,
            "plays_data": plays_data
        })
    except Exception as exc:
        logger.warning(f"Failed to fetch episode plays total for {ep_uri}: {exc}")

# Build enrichment lookup so transforms can access episodeId, duration, etc.
episode_enrichment = {ep.get("uri", ""): ep for ep in raw_episodes}

# Build mapping from Spotify URI -> legacy Anchor web episode ID (e.g. e215pm4)
# using the new legacy API helper in spotifygraphqlconnector.
legacy_web_ids_by_uri: dict[str, str] = {}
legacy_metadata_by_uri: dict[str, dict] = {}
for episode in raw_episodes:
    episode_uri = episode.get("uri", "")
    numeric_episode_id = get_numeric_episode_id(episode)
    if not episode_uri or numeric_episode_id is None:
        continue
    try:
        legacy = connector.get_episode_legacy_web_id(numeric_episode_id)
        legacy_metadata_by_uri[episode_uri] = legacy
        legacy_web_id = legacy.get("webEpisodeId")
        if legacy_web_id:
            legacy_web_ids_by_uri[episode_uri] = legacy_web_id
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            f"Legacy web ID lookup failed for episode {numeric_episode_id} ({episode_uri}): {exc}"
        )

logger.info(
    f"Resolved legacy web IDs for {len(legacy_web_ids_by_uri)}/{len(raw_episodes)} episodes."
)

legacy_web_station_id = next(
    (
        meta.get("webStationId")
        for meta in legacy_metadata_by_uri.values()
        if meta.get("webStationId")
    ),
    "",
)

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
    # Re-using transform_plays_by_geo_city for playsByGeoRegion, as the
    # data shape from the API is identical (list of displayName and value pairs).
    FetchParams(
        openpodcast_endpoint="playsByGeoRegion",
        anchor_call=lambda: transform_plays_by_geo_city(
            geo_stats_region,
            country=geo_city_country,
        ),
        start_date=START_DATE,
        end_date=END_DATE,
    ),
    FetchParams(
        openpodcast_endpoint="playsByGeoCity",
        anchor_call=lambda: transform_plays_by_geo_city(
            geo_stats_city,
            country=geo_city_country,
            region=geo_city_region,
        ),
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
        anchor_call=lambda: transform_unique_listeners(
            discovery_stats,
            fallback_graphql_data=spotify_stats,
        ),
        start_date=START_DATE,
        end_date=END_DATE,
    ),
    FetchParams(
        openpodcast_endpoint="audienceSize",
        anchor_call=lambda: transform_audience_size(
            discovery_stats,
            fallback_graphql_data=spotify_stats,
        ),
        start_date=START_DATE,
        end_date=END_DATE,
    ),
    FetchParams(
        openpodcast_endpoint="totalPlays",
        anchor_call=lambda: transform_total_plays(all_time_show_stats),
        start_date=START_DATE,
        end_date=END_DATE,
    ),
    FetchParams(
        openpodcast_endpoint="totalPlaysByEpisode",
        anchor_call=lambda: transform_total_plays_by_episode(
            all_time_episode_plays, episode_enrichment=episode_enrichment
        ),
        start_date=START_DATE,
        end_date=END_DATE,
    ),
]

# ---------------------------------------------------------------------------
# Episodes
# ---------------------------------------------------------------------------

all_episodes = transform_episodes_page(
    raw_episodes,
    legacy_web_ids_by_uri=legacy_web_ids_by_uri,
    legacy_metadata_by_uri=legacy_metadata_by_uri,
)

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

    legacy_web_id = legacy_web_ids_by_uri.get(episode_uri, episode_uri)
    meta = {"episode": legacy_web_id}

    endpoints += [
        FetchParams(
            openpodcast_endpoint="episodePlays",
            anchor_call=get_request_lambda(
                lambda uri=episode_uri: transform_episode_plays(
                    connector.get_episode_streams_and_downloads(
                        episode_uri=uri,
                        start_date=START_DATE,
                        end_date=END_DATE,
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
                    legacy_web_id=legacy_web_ids_by_uri.get(uri, uri),
                    legacy_episode_data=legacy_metadata_by_uri.get(uri, {}),
                    legacy_web_station_id=legacy_web_station_id,
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
