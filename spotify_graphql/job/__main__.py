"""
Spotify GraphQL pipeline.

Discovers all shows on the account automatically and fetches analytics for
each show and its episodes from the Spotify Creators GraphQL API, then
forwards results to the Open Podcast API.

Required environment variables
--------------------------------
SPOTIFY_SP_DC           sp_dc cookie from an active Spotify session
SPOTIFY_SP_KEY          sp_key cookie from the same session
OPENPODCAST_API_TOKEN   Bearer token for the Open Podcast API

Optional environment variables
--------------------------------
OPENPODCAST_API_ENDPOINT  Default: https://api.openpodcast.dev
DATE_RANGE_WINDOW         WINDOW_LAST_SEVEN_DAYS (default),
                          WINDOW_LAST_THIRTY_DAYS, WINDOW_LAST_NINETY_DAYS,
                          WINDOW_ALL_TIME
NUM_WORKERS               Worker threads (default: 1)
TASK_DELAY                Seconds between requests per worker (default: 1.0)
STORE_DATA                Set to true to also write raw JSON to ./data/
"""

from __future__ import annotations

import datetime as dt
import json
import os
import queue
import sys
import threading
from pathlib import Path

from loguru import logger
from spotifygraphqlconnector import SpotifyGraphQLConnector
from spotifygraphqlconnector.connector import CredentialsExpired

from job.load_env import load_file_or_env
from job.open_podcast import OpenPodcastConnector
from job.worker import FetchParams, worker

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SP_DC = load_file_or_env("SPOTIFY_SP_DC")
SP_KEY = load_file_or_env("SPOTIFY_SP_KEY")
OPENPODCAST_API_ENDPOINT = os.environ.get(
    "OPENPODCAST_API_ENDPOINT", "https://api.openpodcast.dev"
)
OPENPODCAST_API_TOKEN = load_file_or_env("OPENPODCAST_API_TOKEN")

DATE_RANGE_WINDOW = os.environ.get("DATE_RANGE_WINDOW", "WINDOW_LAST_SEVEN_DAYS")
NUM_WORKERS = int(os.environ.get("NUM_WORKERS", "1"))
TASK_DELAY = float(os.environ.get("TASK_DELAY", "1.0"))
STORE_DATA = os.environ.get("STORE_DATA", "false").lower() in ("true", "1", "t")

missing = [
    name
    for name, val in [
        ("SPOTIFY_SP_DC", SP_DC),
        ("SPOTIFY_SP_KEY", SP_KEY),
        ("OPENPODCAST_API_TOKEN", OPENPODCAST_API_TOKEN),
    ]
    if not val
]
if missing:
    logger.error("Missing required environment variables: {}", ", ".join(missing))
    sys.exit(1)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _lam(f, *args, **kwargs):
    """Capture by value so lambdas built inside loops bind correctly."""
    return lambda: f(*args, **kwargs)


def _now() -> dt.datetime:
    return dt.datetime.now()


def _save_locally(show_uri: str, endpoint: str, data: object) -> None:
    """Persist raw response to ./data/<show_uri>/<endpoint>.json for debugging."""
    safe = show_uri.replace(":", "_")
    path = Path("data") / safe / f"{endpoint}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2, default=str)
    logger.debug("Saved {} -> {}", endpoint, path)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    try:
        _run()
    except CredentialsExpired as exc:
        logger.error("Authentication failed - sp_dc/sp_key have expired: {}", exc)
        sys.exit(1)
    except Exception as exc:
        logger.exception("Unexpected error: {}", exc)
        sys.exit(1)


def _run() -> None:
    # One shared connector for all shows; handles PKCE auth and token refresh internally.
    connector = SpotifyGraphQLConnector(sp_dc=SP_DC, sp_key=SP_KEY)

    shows_data = connector.get_shows_for_user()
    shows_for_user = (
        shows_data.get("showsForUser") if isinstance(shows_data, dict) else None
    )
    shows: list = []
    if isinstance(shows_for_user, dict):
        raw = shows_for_user.get("shows", [])
        if isinstance(raw, list):
            shows = raw

    if not shows:
        logger.error("No shows found on this account. Check your credentials.")
        sys.exit(1)

    logger.info("Found {} show(s) on this account.", len(shows))

    for show in shows:
        if not isinstance(show, dict):
            continue
        show_uri = show.get("uri")
        if not isinstance(show_uri, str):
            logger.warning("Skipping show with no URI: {}", show)
            continue
        show_name = show.get("name", show_uri)
        logger.info("--- Processing: {} ({}) ---", show_name, show_uri)
        _process_show(connector, show_uri)


def _process_show(connector: SpotifyGraphQLConnector, show_uri: str) -> None:
    """Build and dispatch the full endpoint list for one show."""

    open_podcast = OpenPodcastConnector(
        OPENPODCAST_API_ENDPOINT,
        OPENPODCAST_API_TOKEN,
        show_uri,
    )

    # Skip the health check when storing data locally - the Open Podcast API
    # is not needed in that mode, so a missing/unreachable API must not abort.
    if not STORE_DATA:
        resp = open_podcast.health()
        if resp.status_code != 200:
            logger.error(
                "Open Podcast API healthcheck failed (HTTP {}). Skipping {}.",
                resp.status_code,
                show_uri,
            )
            return

    # Reset per-show state on the shared connector instance.
    connector.show_uri = show_uri
    connector.station_id = None  # triggers re-resolution for each show

    endpoints: list[FetchParams] = []

    # -- Show-level endpoints -------------------------------------------------

    endpoints += [
        FetchParams(
            openpodcast_endpoint="showSpotifyStats",
            call=_lam(
                connector.get_show_spotify_stats,
                show_uri=show_uri,
                date_range_window=DATE_RANGE_WINDOW,
            ),
            start_date=_now(),
            end_date=_now(),
        ),
        FetchParams(
            openpodcast_endpoint="showGeoStats",
            call=_lam(
                connector.get_show_geo_stats,
                show_uri=show_uri,
                date_range_window=DATE_RANGE_WINDOW,
            ),
            start_date=_now(),
            end_date=_now(),
        ),
        FetchParams(
            openpodcast_endpoint="showDemographicsStats",
            call=_lam(
                connector.get_show_demographics_stats,
                show_uri=show_uri,
                date_range_window=DATE_RANGE_WINDOW,
            ),
            start_date=_now(),
            end_date=_now(),
        ),
        FetchParams(
            openpodcast_endpoint="showPlatformStats",
            call=_lam(
                connector.get_show_platform_stats,
                show_uri=show_uri,
                date_range_window=DATE_RANGE_WINDOW,
            ),
            start_date=_now(),
            end_date=_now(),
        ),
        FetchParams(
            openpodcast_endpoint="showImpressionsTrend",
            call=_lam(
                connector.get_show_impressions_trend,
                show_uri=show_uri,
                date_range_window=DATE_RANGE_WINDOW,
            ),
            start_date=_now(),
            end_date=_now(),
        ),
        FetchParams(
            openpodcast_endpoint="showAudienceDiscovery",
            call=_lam(
                connector.get_show_audience_discovery,
                show_uri=show_uri,
                date_range_window=DATE_RANGE_WINDOW,
            ),
            start_date=_now(),
            end_date=_now(),
        ),
        FetchParams(
            openpodcast_endpoint="showImpressionsSources",
            call=_lam(
                connector.get_show_impressions_sources,
                show_uri=show_uri,
                date_range_window=DATE_RANGE_WINDOW,
            ),
            start_date=_now(),
            end_date=_now(),
        ),
        FetchParams(
            openpodcast_endpoint="showTopEpisodes",
            call=_lam(connector.get_show_top_episodes, show_uri=show_uri),
            start_date=_now(),
            end_date=_now(),
        ),
    ]

    # -- Episode-level endpoints ----------------------------------------------

    logger.info("Fetching all episodes for {}", show_uri)
    try:
        episodes = connector.get_all_episodes()
    except Exception as exc:
        logger.warning(
            "Could not fetch episodes for {} ({}). "
            "This is expected for non-hosted (third-party RSS) shows which have no station ID.",
            show_uri,
            exc,
        )
        episodes = []

    logger.info("{} episode(s) found for {}", len(episodes), show_uri)

    for episode in episodes:
        if not isinstance(episode, dict):
            continue
        ep_uri = episode.get("uri")
        if not isinstance(ep_uri, str) or not ep_uri:
            continue

        meta = {"episode": ep_uri}

        endpoints += [
            FetchParams(
                openpodcast_endpoint="episodeMetadata",
                call=_lam(connector.get_episode_metadata_for_analytics, ep_uri),
                start_date=_now(),
                end_date=_now(),
                meta=meta,
            ),
            FetchParams(
                openpodcast_endpoint="episodePerformanceAllTime",
                call=_lam(connector.get_episode_performance_all_time, ep_uri),
                start_date=_now(),
                end_date=_now(),
                meta=meta,
            ),
            FetchParams(
                openpodcast_endpoint="episodeStreamsAndDownloads",
                call=_lam(
                    connector.get_episode_streams_and_downloads,
                    ep_uri,
                    date_range_window=DATE_RANGE_WINDOW,
                ),
                start_date=_now(),
                end_date=_now(),
                meta=meta,
            ),
            FetchParams(
                openpodcast_endpoint="episodePlaysDaily",
                call=_lam(
                    connector.get_episode_plays_daily,
                    ep_uri,
                    date_range_window=DATE_RANGE_WINDOW,
                ),
                start_date=_now(),
                end_date=_now(),
                meta=meta,
            ),
            FetchParams(
                openpodcast_endpoint="episodeConsumptionAllTime",
                call=_lam(connector.get_episode_consumption_all_time, ep_uri),
                start_date=_now(),
                end_date=_now(),
                meta=meta,
            ),
            FetchParams(
                openpodcast_endpoint="episodeAudienceSizeAllTime",
                call=_lam(connector.get_episode_audience_size_all_time, ep_uri),
                start_date=_now(),
                end_date=_now(),
                meta=meta,
            ),
        ]

    logger.info("Queuing {} API call(s) for {}", len(endpoints), show_uri)

    # STORE_DATA=true: fetch sequentially and write to disk (good for debugging)
    if STORE_DATA:
        for params in endpoints:
            try:
                data = params.call()
                _save_locally(show_uri, params.openpodcast_endpoint, data)
            except Exception as exc:
                logger.error("Failed to fetch {}: {}", params.openpodcast_endpoint, exc)
        return

    # Normal mode: threaded dispatch to the Open Podcast API
    q: queue.Queue = queue.Queue()
    for _ in range(NUM_WORKERS):
        t = threading.Thread(target=worker, args=(q, open_podcast, TASK_DELAY))
        t.daemon = True
        t.start()

    for params in endpoints:
        q.put(params)

    q.join()
    logger.success("Done with show {}.", show_uri)


if __name__ == "__main__":
    main()
