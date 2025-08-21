"""
Apple podcast processor.
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

# Add apple connector path to Python path
apple_path = Path(CONNECTORS_PATH) / "apple"
if str(apple_path) not in sys.path:
    sys.path.insert(0, str(apple_path))

# Import Apple connector modules
from job.fetch_params import FetchParams
from job.worker import worker
from job.dates import get_date_range
import job.apple as apple
from appleconnector import AppleConnector, Metric, Dimension

@huey.task(expires=dt.timedelta(hours=24))
@huey.lock_task('process-apple-podcast-lock')
def process_apple_podcast(podcast: PodcastTask, open_podcast):
    """
    Process Apple podcast fetching task.
    """
    logger.info(f"Processing Apple task for {podcast.pod_name} {podcast.account_id} using podcast_id {podcast.source_podcast_id}")
    
    try:
        
        # Environment setup
        APPLE_AUTOMATION_ENDPOINT = podcast.get_access_key("APPLE_AUTOMATION_ENDPOINT")
        APPLE_AUTOMATION_BEARER_TOKEN = podcast.get_access_key("APPLE_AUTOMATION_BEARER_TOKEN")
        
        # Date range setup
        START_DATE = podcast.get_access_key("START_DATE", (dt.datetime.now() - dt.timedelta(days=7)).strftime("%Y-%m-%d"))
        END_DATE = podcast.get_access_key("END_DATE", dt.datetime.now().strftime("%Y-%m-%d"))
        
        # Extend date range if too short (Apple API requirement)
        if (dt.datetime.strptime(END_DATE, "%Y-%m-%d") - dt.datetime.strptime(START_DATE, "%Y-%m-%d")).days < 30:
            START_DATE = (dt.datetime.strptime(END_DATE, "%Y-%m-%d") - dt.timedelta(days=30)).strftime("%Y-%m-%d")
            logger.info(f"Extended date range to 30 days. New start date: {START_DATE}")
        
        date_range = get_date_range(START_DATE, END_DATE)
        
        # Check required variables
        missing_vars = []
        if not APPLE_AUTOMATION_ENDPOINT: missing_vars.append("APPLE_AUTOMATION_ENDPOINT")
        if not APPLE_AUTOMATION_BEARER_TOKEN: missing_vars.append("APPLE_AUTOMATION_BEARER_TOKEN")
        if not podcast.source_podcast_id: missing_vars.append("APPLE_PODCAST_ID")
        
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
        
        # Get Apple cookies
        logger.info(f"Receiving cookies from Apple automation endpoint")
        cookies = apple.get_cookies(APPLE_AUTOMATION_BEARER_TOKEN, APPLE_AUTOMATION_ENDPOINT)
        
        apple_connector = AppleConnector(
            podcast_id=podcast.source_podcast_id,
            myacinfo=cookies.myacinfo,
            itctx=cookies.itctx,
        )
        
        def get_request_lambda(f, *args, **kwargs):
            return lambda: f(*args, **kwargs)
        
        # Build endpoints
        endpoints = []
        DAYS_PER_CHUNK = int(podcast.get_access_key("DAYS_PER_CHUNK", 4 * 30))
        
        for chunk_id, (start_date, end_date) in enumerate(date_range.chunks(DAYS_PER_CHUNK)):
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
                FetchParams(
                    openpodcast_endpoint="showTrends/ListeningTimeFollowerState",
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
            ]
        
        endpoints.append(
            FetchParams(
                openpodcast_endpoint="episodes",
                call=lambda: apple_connector.episodes(),
                start_date=date_range.start,
                end_date=date_range.end,
            )
        )
        
        # Add episode endpoints
        episodes = apple.get_episode_ids(apple_connector)
        for episode_id in episodes:
            endpoints.append(
                FetchParams(
                    openpodcast_endpoint="episodeDetails",
                    call=get_request_lambda(apple_connector.episode, episode_id),
                    start_date=date_range.start,
                    end_date=date_range.end,
                    meta={"episode": episode_id},
                )
            )
        
        # Process with worker threads
        queue = Queue()
        NUM_WORKERS = int(os.environ.get("NUM_WORKERS", 1))
        TASK_DELAY = float(os.environ.get("TASK_DELAY", 0))
        
        for i in range(NUM_WORKERS):
            t = threading.Thread(target=worker, args=(queue, open_podcast, TASK_DELAY))
            t.daemon = True
            t.start()
        
        for endpoint in endpoints:
            queue.put(endpoint)
        
        queue.join()
        
        logger.info(f"Successfully processed Apple podcast {podcast.pod_name} {podcast.account_id}")
        return {
            "status": "success",
            "account_id": podcast.account_id,
            "source_name": "apple",
            "podcast_id": podcast.source_podcast_id,
            "pod_name": podcast.pod_name,
            "endpoints_processed": len(endpoints)
        }
        
    except Exception as e:
        logger.error(f"Exception while processing Apple podcast {podcast.pod_name}: {e}")
        raise