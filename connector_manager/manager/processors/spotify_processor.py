"""
Spotify podcast processor.
"""
import os
import sys
import datetime as dt
from pathlib import Path
from loguru import logger

from manager.huey_config import huey
from .dto import PodcastTask
from manager.load_env import load_env
from manager.async_worker import run_async_workers

# Environment variables
CONNECTORS_PATH = load_env("CONNECTORS_PATH", ".")

# Add spotify connector path to Python path
spotify_path = Path(CONNECTORS_PATH) / "spotify"
if str(spotify_path) not in sys.path:
    sys.path.insert(0, str(spotify_path))

# Import Spotify connector modules
from job.fetch_params import FetchParams
from job.dates import get_date_range
from job.spotify import get_episode_release_date
from spotifyconnector import SpotifyConnector
from spotifyconnector.connector import CredentialsExpired

@huey.task(expires=dt.timedelta(hours=24))
@huey.lock_task('process-spotify-podcast-lock')
def process_spotify_podcast(podcast: PodcastTask, open_podcast):
    """
    Process Spotify podcast fetching task.
    """
    logger.info(f"Processing Spotify task for {podcast.pod_name} {podcast.account_id} using podcast_id {podcast.source_podcast_id}")
    
    try:
        
        # Date range setup
        START_DATE = podcast.get_access_key("START_DATE", (dt.datetime.now() - dt.timedelta(days=4)).strftime("%Y-%m-%d"))
        END_DATE = podcast.get_access_key("END_DATE", (dt.datetime.now() - dt.timedelta(days=1)).strftime("%Y-%m-%d"))
        date_range = get_date_range(START_DATE, END_DATE)
        
        # Check required variables
        missing_vars = []
        if not podcast.get_access_key("SPOTIFY_SP_DC"): missing_vars.append("SPOTIFY_SP_DC")
        if not podcast.get_access_key("SPOTIFY_SP_KEY"): missing_vars.append("SPOTIFY_SP_KEY") 
        if not podcast.source_podcast_id: missing_vars.append("PODCAST_ID")
        if not podcast.openpodcast_api_token: missing_vars.append("OPENPODCAST_API_TOKEN")
        
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
        
        # Initialize Spotify connector
        spotify = SpotifyConnector(
            base_url=podcast.get_access_key("SPOTIFY_BASE_URL", "https://generic.wg.spotify.com/podcasters/v0"),
            client_id=podcast.get_access_key("SPOTIFY_CLIENT_ID", "05a1371ee5194c27860b3ff3ff3979d2"),
            podcast_id=podcast.source_podcast_id,
            sp_dc=podcast.get_access_key("SPOTIFY_SP_DC"),
            sp_key=podcast.get_access_key("SPOTIFY_SP_KEY"),
        )
        
        def get_request_lambda(f, *args, **kwargs):
            return lambda: f(*args, **kwargs)
        
        # Build endpoints list (same logic as original __main__.py)
        todayDate = dt.datetime.now()
        yesterdayDate = todayDate - dt.timedelta(days=1)
        oldestDate = dt.datetime(2015, 5, 1)
        
        endpoints = [
            FetchParams(
                openpodcast_endpoint="metadata",
                spotify_call=lambda: spotify.metadata(),
                start_date=date_range.start,
                end_date=date_range.end,
            ),
            FetchParams(
                openpodcast_endpoint="listeners",
                spotify_call=get_request_lambda(spotify.listeners, date_range.start, date_range.end),
                start_date=date_range.start,
                end_date=date_range.end,
            ),
            FetchParams(
                openpodcast_endpoint="detailedStreams",
                spotify_call=get_request_lambda(spotify.streams, date_range.start, date_range.end),
                start_date=date_range.start,
                end_date=date_range.end,
            ),
            FetchParams(
                openpodcast_endpoint="followers",
                spotify_call=get_request_lambda(spotify.followers, date_range.start, date_range.end),
                start_date=date_range.start,
                end_date=date_range.end,
            ),
        ] + [
            FetchParams(
                openpodcast_endpoint="aggregate",
                spotify_call=get_request_lambda(spotify.aggregate, current_date, current_date),
                start_date=current_date,
                end_date=current_date,
            )
            for current_date in date_range
        ]
        
        # Fetch episodes and add their endpoints
        episodes = spotify.episodes(oldestDate, todayDate)
        
        for episode in episodes:
            episode_id = episode["id"]
            
            endpoints += [
                FetchParams(
                    openpodcast_endpoint="episodeMetadata",
                    spotify_call=get_request_lambda(spotify.metadata, episode=episode_id),
                    start_date=date_range.start,
                    end_date=date_range.end,
                    meta={"episode": episode_id},
                ),
                FetchParams(
                    openpodcast_endpoint="detailedStreams",
                    spotify_call=get_request_lambda(spotify.streams, date_range.start, date_range.end, episode=episode_id),
                    start_date=date_range.start,
                    end_date=date_range.end,
                    meta={"episode": episode_id},
                ),
                FetchParams(
                    openpodcast_endpoint="listeners",
                    spotify_call=get_request_lambda(spotify.listeners, date_range.start, date_range.end, episode=episode_id),
                    start_date=date_range.start,
                    end_date=date_range.end,
                    meta={"episode": episode_id},
                ),
                FetchParams(
                    openpodcast_endpoint="performance",
                    spotify_call=get_request_lambda(spotify.performance, episode=episode_id),
                    start_date=date_range.start,
                    end_date=date_range.end,
                    meta={"episode": episode_id},
                ),
            ]
            
            # Episode date-specific endpoints
            release_date = get_episode_release_date(episode)
            if release_date:
                episode_start_date = max(release_date, date_range.start)
                episode_end_date = date_range.end
                
                if episode_end_date >= episode_start_date:
                    episode_date_range = get_date_range(
                        episode_start_date.strftime("%Y-%m-%d"),
                        episode_end_date.strftime("%Y-%m-%d")
                    )
                    
                    endpoints += [
                        FetchParams(
                            openpodcast_endpoint="aggregate",
                            spotify_call=get_request_lambda(spotify.aggregate, current_date, current_date, episode=episode_id),
                            start_date=current_date,
                            end_date=current_date,
                            meta={"episode": episode_id},
                        )
                        for current_date in episode_date_range
                    ]
        
        # Process all endpoints with async workers
        NUM_WORKERS = int(os.environ.get("NUM_WORKERS", 4))
        TASK_DELAY = float(os.environ.get("TASK_DELAY", 1.5))
        
        # Run async workers
        run_async_workers(endpoints, open_podcast, NUM_WORKERS, TASK_DELAY)
        
        logger.info(f"Successfully processed Spotify podcast {podcast.pod_name} {podcast.account_id}")
        return {
            "status": "success",
            "account_id": podcast.account_id,
            "source_name": "spotify", 
            "podcast_id": podcast.source_podcast_id,
            "pod_name": podcast.pod_name,
            "endpoints_processed": len(endpoints)
        }
        
    except CredentialsExpired as e:
        logger.error(f"Spotify credentials expired for {podcast.pod_name} {podcast.account_id}: {e}")
        raise
    except Exception as e:
        logger.error(f"Exception while processing Spotify podcast {podcast.pod_name}: {e}")
        raise