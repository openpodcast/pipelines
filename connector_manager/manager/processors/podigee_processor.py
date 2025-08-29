"""
Podigee podcast processor.
"""
import os
import sys
import datetime as dt
from pathlib import Path
from loguru import logger
from queue import Queue
import threading

from manager.huey_config import huey
from .dto import PodcastTask
from manager.load_env import load_env

# Environment variables
CONNECTORS_PATH = load_env("CONNECTORS_PATH", ".")

# Add podigee connector path to Python path
podigee_path = Path(CONNECTORS_PATH) / "podigee"
if str(podigee_path) not in sys.path:
    sys.path.insert(0, str(podigee_path))

# Import Podigee connector modules
from job.fetch_params import FetchParams
from job.worker import worker
from job.dates import get_date_range
from podigeeconnector import PodigeeConnector

@huey.task(expires=dt.timedelta(hours=24)) 
@huey.lock_task('process-podigee-podcast-lock')
def process_podigee_podcast(podcast: PodcastTask, open_podcast):
    """
    Process Podigee podcast fetching task.
    """
    logger.info(f"Processing Podigee task for {podcast.pod_name} {podcast.account_id} using podcast_id {podcast.source_podcast_id}")
    
    try:
        
        # Default environment setup
        BASE_URL = podcast.get_access_key("PODIGEE_BASE_URL", "https://app.podigee.com/api/v1")
        PODIGEE_ACCESS_TOKEN = podcast.get_access_key("PODIGEE_ACCESS_TOKEN")
        PODIGEE_USERNAME = podcast.get_access_key("PODIGEE_USERNAME")
        PODIGEE_PASSWORD = podcast.get_access_key("PODIGEE_PASSWORD")
        
        # Date range setup
        START_DATE = podcast.get_access_key("START_DATE", (dt.datetime.now() - dt.timedelta(days=30)).strftime("%Y-%m-%d"))
        END_DATE = podcast.get_access_key("END_DATE", (dt.datetime.now() - dt.timedelta(days=1)).strftime("%Y-%m-%d"))
        date_range = get_date_range(START_DATE, END_DATE)
        
        # Check required variables
        if not podcast.source_podcast_id:
            raise ValueError("Missing required PODCAST_ID")
            
        # Check authentication
        has_api_token = PODIGEE_ACCESS_TOKEN is not None
        has_credentials = PODIGEE_USERNAME is not None and PODIGEE_PASSWORD is not None
        
        if not has_api_token and not has_credentials:
            raise ValueError("Missing Podigee authentication. Need either PODIGEE_ACCESS_TOKEN or PODIGEE_USERNAME+PODIGEE_PASSWORD")
        
        # Convert podcast ID to int
        try:
            PODCAST_ID = int(podcast.source_podcast_id)
        except ValueError:
            raise ValueError(f"PODCAST_ID must be an integer, got: {podcast.source_podcast_id}")
        
        # Initialize Podigee connector
        if has_api_token:
            logger.info("Using Podigee API token for authentication")
            podigee = PodigeeConnector(
                base_url=BASE_URL,
                podigee_access_token=PODIGEE_ACCESS_TOKEN,
            )
        else:
            logger.info("Using Podigee username/password for authentication")
            podigee = PodigeeConnector.from_credentials(
                base_url=BASE_URL,
                username=PODIGEE_USERNAME,
                password=PODIGEE_PASSWORD,
            )
        
        # Validate podcast exists
        podcasts = podigee.podcasts()
        podcast_data = None
        for p in podcasts:
            if p["id"] == PODCAST_ID:
                podcast_data = p
                break
                
        if not podcast_data:
            available_ids = [p['id'] for p in podcasts]
            raise ValueError(f"Podcast with ID {PODCAST_ID} not found. Available: {available_ids}")
        
        podcast_title = podcast_data.get("title")
        if not podcast_title:
            raise ValueError(f"Podcast with ID {PODCAST_ID} has no title")
        
        
        def get_request_lambda(f, *args, **kwargs):
            return lambda: f(*args, **kwargs)
        
        def get_podcast_metadata():
            return {"name": podcast_title}
        
        def transform_podigee_analytics_to_metrics(analytics_data):
            if not analytics_data or "objects" not in analytics_data:
                return {"metrics": []}
            
            metrics = []
            for day_data in analytics_data["objects"]:
                date = day_data.get("downloaded_on", "").split("T")[0]
                if not date:
                    continue
                    
                # Process various metrics
                for metric_type in ["downloads", "platforms", "clients", "sources"]:
                    if metric_type in day_data:
                        for subdimension, value in day_data[metric_type].items():
                            metrics.append({
                                "start": date,
                                "end": date, 
                                "dimension": metric_type,
                                "subdimension": subdimension,
                                "value": value
                            })
            
            return {"metrics": metrics}
        
        # Build endpoints
        endpoints = [
            FetchParams(
                openpodcast_endpoint="metadata",
                podigee_call=get_podcast_metadata,
                start_date=date_range.start,
                end_date=date_range.end,
            ),
            FetchParams(
                openpodcast_endpoint="metrics",
                podigee_call=lambda: transform_podigee_analytics_to_metrics(
                    podigee.podcast_analytics(PODCAST_ID, start=date_range.start, end=date_range.end)
                ),
                start_date=date_range.start,
                end_date=date_range.end,
            ),
        ]
        
        # Add episode endpoints
        episodes = podigee.episodes(PODCAST_ID)
        for episode in episodes:
            endpoints += [
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
        
        logger.info(f"Successfully processed Podigee podcast {podcast.pod_name} {podcast.account_id}")
        return {
            "status": "success",
            "account_id": podcast.account_id,
            "source_name": "podigee",
            "podcast_id": podcast.source_podcast_id,
            "pod_name": podcast.pod_name,
            "endpoints_processed": len(endpoints)
        }
        
    except Exception as e:
        logger.error(f"Exception while processing Podigee podcast {podcast.pod_name}: {e}")
        raise