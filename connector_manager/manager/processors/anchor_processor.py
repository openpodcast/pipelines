"""
Anchor podcast processor.
"""
import os
import sys
import datetime as dt
from pathlib import Path
from loguru import logger
import requests
from queue import Queue
import threading

from manager.huey_config import huey
from .dto import PodcastTask
from manager.load_env import load_env

# Environment variables
CONNECTORS_PATH = load_env("CONNECTORS_PATH", ".")

# Add anchor connector path to Python path
anchor_path = Path(CONNECTORS_PATH) / "anchor"
if str(anchor_path) not in sys.path:
    sys.path.insert(0, str(anchor_path))

# Import Anchor connector modules
from job.fetch_params import FetchParams
from job.worker import worker
from job.dates import get_date_range
from anchorconnector import AnchorConnector

@huey.task(expires=dt.timedelta(hours=24))
@huey.lock_task('process-anchor-podcast-lock')
def process_anchor_podcast(podcast: PodcastTask, open_podcast):
    """
    Process Anchor podcast fetching task.
    """
    logger.info(f"Processing Anchor task for {podcast.pod_name} {podcast.account_id} using podcast_id {podcast.source_podcast_id}")
    
    try:
        
        # Environment setup
        BASE_URL = podcast.get_access_key("ANCHOR_BASE_URL", "https://podcasters.spotify.com/pod/api/proxy/v3")
        ANCHOR_WEBSTATION_ID = podcast.get_access_key("ANCHOR_WEBSTATION_ID", podcast.source_podcast_id)
        ANCHOR_PW_S = podcast.get_access_key("ANCHOR_PW_S")
        
        # Date range setup
        START_DATE = podcast.get_access_key("START_DATE", (dt.datetime.now() - dt.timedelta(days=30)).strftime("%Y-%m-%d"))
        END_DATE = podcast.get_access_key("END_DATE", (dt.datetime.now() - dt.timedelta(days=1)).strftime("%Y-%m-%d"))
        date_range = get_date_range(START_DATE, END_DATE)
        
        # Check required variables
        missing_vars = []
        if not ANCHOR_WEBSTATION_ID: missing_vars.append("ANCHOR_WEBSTATION_ID")
        if not ANCHOR_PW_S: missing_vars.append("ANCHOR_PW_S")
        
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
        
        # Initialize Anchor connector
        anchor = AnchorConnector(
            base_url=BASE_URL,
            base_graphql_url="http://example.com",
            webstation_id=ANCHOR_WEBSTATION_ID,
            anchorpw_s=ANCHOR_PW_S,
        )
        
        def get_request_lambda(f, *args, **kwargs):
            return lambda: f(*args, **kwargs)
        
        def episode_all_time_video_data(connector, web_episode_id):
            try:
                return anchor.episode_all_time_video_data(web_episode_id)
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    logger.info("Episode has no video data or URL is incorrect")
                    return None
                else:
                    raise
        
        def wrap_episode_metadata(data):
            """Transform episode data to match expected format"""
            transformed_episode = {
                "adCount": 0,
                "created": data.get("created"),
                "createdUnixTimestamp": data.get("createdUnixTimestamp"),
                "description": data.get("description"),
                "duration": data.get("totalDuration"),
                "hourOffset": data.get("hourOffset"),
                "isDeleted": data.get("isDeleted", False),
                "isPublished": data.get("isPublished", False),
                "podcastEpisodeId": data.get("webEpisodeId"),
                "publishOn": data.get("publishOn"),
                "publishOnUnixTimestamp": data.get("publishOnUnixTimestamp", 0) * 1000,
                "title": data.get("title"),
                "url": (
                    data.get("episodeAudios", [{}])[0].get("url")
                    if data.get("episodeAudios")
                    else None
                ),
                "trackedUrl": data.get("spotifyUrl"),
                "episodeImage": data.get("episodeImage"),
                "shareLinkPath": data.get("shareLinkPath"),
                "shareLinkEmbedPath": data.get("shareLinkEmbedPath"),
            }
            
            return {
                "allEpisodeWebIds": [data.get("webEpisodeId")],
                "podcastId": data.get("webStationId"),
                "podcastEpisodes": [transformed_episode],
                "totalPodcastEpisodes": 1,
                "vanitySlug": "dummy",
                "stationCreatedDate": "dummy",
            }
        
        # Build main endpoints
        endpoints = [
            FetchParams(
                openpodcast_endpoint="plays",
                anchor_call=get_request_lambda(anchor.plays, date_range.start, date_range.end),
                start_date=date_range.start,
                end_date=date_range.end,
            ),
            FetchParams(
                openpodcast_endpoint="playsByAgeRange",
                anchor_call=get_request_lambda(anchor.plays_by_age_range, date_range.start, date_range.end),
                start_date=date_range.start,
                end_date=date_range.end,
            ),
            FetchParams(
                openpodcast_endpoint="playsByApp",
                anchor_call=get_request_lambda(anchor.plays_by_app, date_range.start, date_range.end),
                start_date=date_range.start,
                end_date=date_range.end,
            ),
            FetchParams(
                openpodcast_endpoint="playsByDevice",
                anchor_call=get_request_lambda(anchor.plays_by_device, date_range.start, date_range.end),
                start_date=date_range.start,
                end_date=date_range.end,
            ),
            FetchParams(
                openpodcast_endpoint="playsByGender",
                anchor_call=get_request_lambda(anchor.plays_by_gender, date_range.start, date_range.end),
                start_date=date_range.start,
                end_date=date_range.end,
            ),
            FetchParams(
                openpodcast_endpoint="playsByGeo",
                anchor_call=get_request_lambda(anchor.plays_by_geo),
                start_date=date_range.start,
                end_date=date_range.end,
            ),
            FetchParams(
                openpodcast_endpoint="uniqueListeners",
                anchor_call=anchor.unique_listeners,
                start_date=date_range.start,
                end_date=date_range.end,
            ),
            FetchParams(
                openpodcast_endpoint="audienceSize",
                anchor_call=anchor.audience_size,
                start_date=date_range.start,
                end_date=date_range.end,
            ),
            FetchParams(
                openpodcast_endpoint="totalPlaysByEpisode",
                anchor_call=anchor.total_plays_by_episode,
                start_date=date_range.start,
                end_date=date_range.end,
            ),
            FetchParams(
                openpodcast_endpoint="totalPlays",
                anchor_call=get_request_lambda(anchor.total_plays, True),
                start_date=date_range.start,
                end_date=date_range.end,
            ),
        ]
        
        # Add geo city endpoints
        countries = anchor.plays_by_geo()
        for row in countries["data"]["rows"]:
            country = row[0]
            endpoints.append(
                FetchParams(
                    openpodcast_endpoint="playsByGeoCity",
                    anchor_call=get_request_lambda(anchor.plays_by_geo_city, country),
                    start_date=date_range.start,
                    end_date=date_range.end,
                    meta={"country": country},
                )
            )
        
        # Get episodes and send metadata
        episodes = anchor.episodes()
        all_episodes = list(episodes)
        
        logger.info("Sending episodesPage data to Open Podcast")
        open_podcast.post("episodesPage", None, all_episodes, date_range.start, date_range.end)
        
        # Add episode-specific endpoints
        for episode in all_episodes:
            web_episode_id = episode["webEpisodeId"]
            meta = {
                "episode": web_episode_id,
                "episodeIdNum": episode["episodeId"],
                "webEpisodeId": web_episode_id,
            }
            
            endpoints += [
                FetchParams(
                    openpodcast_endpoint="episodePlays",
                    anchor_call=get_request_lambda(
                        anchor.episode_plays,
                        web_episode_id,
                        date_range.start,
                        date_range.end,
                        "daily",
                    ),
                    start_date=date_range.start,
                    end_date=date_range.end,
                    meta=meta,
                ),
                FetchParams(
                    openpodcast_endpoint="episodePerformance",
                    anchor_call=get_request_lambda(anchor.episode_performance, web_episode_id),
                    start_date=date_range.start,
                    end_date=date_range.end,
                    meta=meta,
                ),
                FetchParams(
                    openpodcast_endpoint="aggregatedPerformance",
                    anchor_call=get_request_lambda(anchor.episode_aggregated_performance, web_episode_id),
                    start_date=date_range.start,
                    end_date=date_range.end,
                    meta=meta,
                ),
                FetchParams(
                    openpodcast_endpoint="podcastEpisode",
                    anchor_call=(
                        lambda episode_id=web_episode_id: wrap_episode_metadata(
                            get_request_lambda(anchor.episode_metadata, episode_id)()
                        )
                    ),
                    start_date=date_range.start,
                    end_date=date_range.end,
                    meta=meta,
                ),
            ]
        
        # Process with worker threads
        queue = Queue()
        NUM_WORKERS = int(os.environ.get("NUM_WORKERS", 1))
        
        for i in range(NUM_WORKERS):
            t = threading.Thread(target=worker, args=(queue, open_podcast))
            t.daemon = True
            t.start()
        
        for endpoint in endpoints:
            queue.put(endpoint)
        
        queue.join()
        
        logger.info(f"Successfully processed Anchor podcast {podcast.pod_name} {podcast.account_id}")
        return {
            "status": "success",
            "account_id": podcast.account_id,
            "source_name": "anchor",
            "podcast_id": podcast.source_podcast_id,
            "pod_name": podcast.pod_name,
            "endpoints_processed": len(endpoints)
        }
        
    except Exception as e:
        logger.error(f"Exception while processing Anchor podcast {podcast.pod_name}: {e}")
        raise