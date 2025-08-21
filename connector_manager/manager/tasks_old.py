import os
import sys
import datetime as dt
from pathlib import Path
from loguru import logger
import mysql.connector
from huey import crontab
from manager.huey_config import huey
from manager.load_env import load_env, load_file_or_env
from manager.cryptography import decrypt_json
from manager.podigee_connector import handle_podigee_refresh
from manager.processors.dto import PodcastTask
from manager.processors.spotify_processor import process_spotify_podcast
from manager.processors.podigee_processor import process_podigee_podcast
from manager.processors.apple_processor import process_apple_podcast
from manager.processors.anchor_processor import process_anchor_podcast

# Environment variables
CONNECTORS_PATH = load_env("CONNECTORS_PATH", ".")
MYSQL_HOST = load_env("MYSQL_HOST", "localhost")
MYSQL_PORT = load_env("MYSQL_PORT", 3306)
MYSQL_USER = load_env("MYSQL_USER", "root")
MYSQL_PASSWORD = load_file_or_env("MYSQL_PASSWORD")
MYSQL_DATABASE = load_env("MYSQL_DATABASE", "openpodcast_auth")
OPENPODCAST_ENCRYPTION_KEY = load_file_or_env("OPENPODCAST_ENCRYPTION_KEY")

# Podigee-specific environment variables
PODIGEE_CLIENT_ID = load_env("PODIGEE_CLIENT_ID")
PODIGEE_CLIENT_SECRET = load_file_or_env("PODIGEE_CLIENT_SECRET")
PODIGEE_REDIRECT_URI = load_env("PODIGEE_REDIRECT_URI", 
                               "https://connect.openpodcast.app/auth/v1/podigee/callback")

# Spotify processor is imported from manager.processors.spotify_processor

@huey.task(expires=dt.timedelta(hours=24)) 
@huey.lock_task('process-podigee-podcast-lock')
def process_podigee_podcast(podcast: PodcastTask):
    """
    Process Podigee podcast fetching task.
    """
    logger.info(f"Processing Podigee task for {podcast.pod_name} {podcast.account_id} using podcast_id {podcast.source_podcast_id}")
    
    # Add podigee connector path to Python path
    podigee_path = Path(CONNECTORS_PATH) / "podigee"
    sys.path.insert(0, str(podigee_path))
    
    try:
        # Import Podigee connector modules
        from job.fetch_params import FetchParams
        from job.worker import worker
        from job.open_podcast import OpenPodcastConnector  
        from job.dates import get_date_range
        from podigeeconnector import PodigeeConnector
        from queue import Queue
        import threading
        
        # Set environment variables from access keys
        for key, value in podcast.source_access_keys.items():
            os.environ[key] = str(value)
            
        os.environ["PODCAST_ID"] = podcast.source_podcast_id
        os.environ["PODCAST_NAME"] = podcast.pod_name
        
        # Default environment setup
        OPENPODCAST_API_ENDPOINT = os.environ.get("OPENPODCAST_API_ENDPOINT", "https://api.openpodcast.dev")
        OPENPODCAST_API_TOKEN = podcast.get_access_key("OPENPODCAST_API_TOKEN")
        BASE_URL = podcast.get_access_key("PODIGEE_BASE_URL", "https://app.podigee.com/api/v1")
        PODIGEE_ACCESS_TOKEN = podcast.get_access_key("PODIGEE_ACCESS_TOKEN")
        PODIGEE_USERNAME = podcast.get_access_key("PODIGEE_USERNAME")
        PODIGEE_PASSWORD = podcast.get_access_key("PODIGEE_PASSWORD")
        
        # Date range setup
        START_DATE = podcast.get_access_key("START_DATE", (dt.datetime.now() - dt.timedelta(days=30)).strftime("%Y-%m-%d"))
        END_DATE = podcast.get_access_key("END_DATE", (dt.datetime.now() - dt.timedelta(days=1)).strftime("%Y-%m-%d"))
        date_range = get_date_range(START_DATE, END_DATE)
        
        # Check required variables
        if not OPENPODCAST_API_TOKEN:
            raise ValueError("Missing required OPENPODCAST_API_TOKEN")
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
        podcast = None
        for p in podcasts:
            if p["id"] == PODCAST_ID:
                podcast = p
                break
                
        if not podcast:
            available_ids = [p['id'] for p in podcasts]
            raise ValueError(f"Podcast with ID {PODCAST_ID} not found. Available: {available_ids}")
        
        podcast_title = podcast.get("title")
        if not podcast_title:
            raise ValueError(f"Podcast with ID {PODCAST_ID} has no title")
        
        # Initialize OpenPodcast connector
        open_podcast = OpenPodcastConnector(
            OPENPODCAST_API_ENDPOINT,
            OPENPODCAST_API_TOKEN,
            PODCAST_ID
        )
        
        # Health check
        response = open_podcast.health()
        if response.status_code != 200:
            raise Exception(f"Open Podcast API healthcheck failed with status code {response.status_code}")
        
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
    finally:
        # Clean up sys.path
        if str(podigee_path) in sys.path:
            sys.path.remove(str(podigee_path))

@huey.task(expires=dt.timedelta(hours=24))
@huey.lock_task('process-apple-podcast-lock')
def process_apple_podcast(podcast: PodcastTask):
    """
    Process Apple podcast fetching task.
    """
    logger.info(f"Processing Apple task for {podcast.pod_name} {podcast.account_id} using podcast_id {podcast.source_podcast_id}")
    
    # Add apple connector path to Python path
    apple_path = Path(CONNECTORS_PATH) / "apple"
    sys.path.insert(0, str(apple_path))
    
    try:
        # Import Apple connector modules
        from job.fetch_params import FetchParams
        from job.worker import worker
        from job.open_podcast import OpenPodcastConnector
        from job.dates import get_date_range
        import job.apple as apple
        from appleconnector import AppleConnector, Metric, Dimension
        from queue import Queue
        import threading
        
        # Set environment variables from access keys
        for key, value in source_access_keys.items():
            os.environ[key] = str(value)
            
        os.environ["PODCAST_ID"] = source_podcast_id
        os.environ["PODCAST_NAME"] = pod_name
        os.environ["APPLE_PODCAST_ID"] = source_access_keys.get("APPLE_PODCAST_ID", source_podcast_id)
        
        # Environment setup
        OPENPODCAST_API_ENDPOINT = os.environ.get("OPENPODCAST_API_ENDPOINT", "https://api.openpodcast.dev")
        OPENPODCAST_API_TOKEN = source_access_keys.get("OPENPODCAST_API_TOKEN")
        APPLE_AUTOMATION_ENDPOINT = source_access_keys.get("APPLE_AUTOMATION_ENDPOINT")
        APPLE_AUTOMATION_BEARER_TOKEN = source_access_keys.get("APPLE_AUTOMATION_BEARER_TOKEN")
        
        # Date range setup
        START_DATE = source_access_keys.get("START_DATE", (dt.datetime.now() - dt.timedelta(days=7)).strftime("%Y-%m-%d"))
        END_DATE = source_access_keys.get("END_DATE", dt.datetime.now().strftime("%Y-%m-%d"))
        
        # Extend date range if too short (Apple API requirement)
        if (dt.datetime.strptime(END_DATE, "%Y-%m-%d") - dt.datetime.strptime(START_DATE, "%Y-%m-%d")).days < 30:
            START_DATE = (dt.datetime.strptime(END_DATE, "%Y-%m-%d") - dt.timedelta(days=30)).strftime("%Y-%m-%d")
            logger.info(f"Extended date range to 30 days. New start date: {START_DATE}")
        
        date_range = get_date_range(START_DATE, END_DATE)
        
        # Check required variables
        missing_vars = []
        if not APPLE_AUTOMATION_ENDPOINT: missing_vars.append("APPLE_AUTOMATION_ENDPOINT")
        if not APPLE_AUTOMATION_BEARER_TOKEN: missing_vars.append("APPLE_AUTOMATION_BEARER_TOKEN")
        if not source_podcast_id: missing_vars.append("APPLE_PODCAST_ID")
        if not OPENPODCAST_API_TOKEN: missing_vars.append("OPENPODCAST_API_TOKEN")
        
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
        
        # Initialize connectors
        open_podcast = OpenPodcastConnector(
            OPENPODCAST_API_ENDPOINT,
            OPENPODCAST_API_TOKEN,
            source_podcast_id,
        )
        
        # Health check
        response = open_podcast.health()
        if response.status_code != 200:
            raise Exception(f"Open Podcast API healthcheck failed with status code {response.status_code}")
        
        # Get Apple cookies
        logger.info(f"Receiving cookies from Apple automation endpoint")
        cookies = apple.get_cookies(APPLE_AUTOMATION_BEARER_TOKEN, APPLE_AUTOMATION_ENDPOINT)
        
        apple_connector = AppleConnector(
            podcast_id=source_podcast_id,
            myacinfo=cookies.myacinfo,
            itctx=cookies.itctx,
        )
        
        def get_request_lambda(f, *args, **kwargs):
            return lambda: f(*args, **kwargs)
        
        # Build endpoints
        endpoints = []
        DAYS_PER_CHUNK = int(source_access_keys.get("DAYS_PER_CHUNK", 4 * 30))
        
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
        
        logger.info(f"Successfully processed Apple podcast {pod_name} {account_id}")
        return {
            "status": "success",
            "account_id": account_id,
            "source_name": "apple",
            "podcast_id": source_podcast_id,
            "pod_name": pod_name,
            "endpoints_processed": len(endpoints)
        }
        
    except Exception as e:
        logger.error(f"Exception while processing Apple podcast {pod_name}: {e}")
        raise
    finally:
        # Clean up sys.path
        if str(apple_path) in sys.path:
            sys.path.remove(str(apple_path))

@huey.task(expires=dt.timedelta(hours=24))
@huey.lock_task('process-anchor-podcast-lock')
def process_anchor_podcast(podcast: PodcastTask):
    """
    Process Anchor podcast fetching task.
    """
    logger.info(f"Processing Anchor task for {podcast.pod_name} {podcast.account_id} using podcast_id {podcast.source_podcast_id}")
    
    # Add anchor connector path to Python path
    anchor_path = Path(CONNECTORS_PATH) / "anchor"
    sys.path.insert(0, str(anchor_path))
    
    try:
        # Import Anchor connector modules
        from job.fetch_params import FetchParams
        from job.worker import worker
        from job.open_podcast import OpenPodcastConnector
        from job.dates import get_date_range
        from anchorconnector import AnchorConnector
        from queue import Queue
        import threading
        import requests
        
        # Set environment variables from access keys
        for key, value in source_access_keys.items():
            os.environ[key] = str(value)
            
        os.environ["PODCAST_ID"] = source_podcast_id
        os.environ["PODCAST_NAME"] = pod_name
        os.environ["ANCHOR_WEBSTATION_ID"] = source_access_keys.get("ANCHOR_WEBSTATION_ID", source_podcast_id)
        
        # Environment setup
        OPENPODCAST_API_ENDPOINT = os.environ.get("OPENPODCAST_API_ENDPOINT", "https://api.openpodcast.dev")
        OPENPODCAST_API_TOKEN = source_access_keys.get("OPENPODCAST_API_TOKEN")
        BASE_URL = source_access_keys.get("ANCHOR_BASE_URL", "https://podcasters.spotify.com/pod/api/proxy/v3")
        ANCHOR_WEBSTATION_ID = source_access_keys.get("ANCHOR_WEBSTATION_ID", source_podcast_id)
        ANCHOR_PW_S = source_access_keys.get("ANCHOR_PW_S")
        
        # Date range setup
        START_DATE = source_access_keys.get("START_DATE", (dt.datetime.now() - dt.timedelta(days=30)).strftime("%Y-%m-%d"))
        END_DATE = source_access_keys.get("END_DATE", (dt.datetime.now() - dt.timedelta(days=1)).strftime("%Y-%m-%d"))
        date_range = get_date_range(START_DATE, END_DATE)
        
        # Check required variables
        missing_vars = []
        if not OPENPODCAST_API_TOKEN: missing_vars.append("OPENPODCAST_API_TOKEN")
        if not ANCHOR_WEBSTATION_ID: missing_vars.append("ANCHOR_WEBSTATION_ID")
        if not ANCHOR_PW_S: missing_vars.append("ANCHOR_PW_S")
        
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
        
        # Initialize connectors
        anchor = AnchorConnector(
            base_url=BASE_URL,
            base_graphql_url="http://example.com",
            webstation_id=ANCHOR_WEBSTATION_ID,
            anchorpw_s=ANCHOR_PW_S,
        )
        
        open_podcast = OpenPodcastConnector(
            OPENPODCAST_API_ENDPOINT,
            OPENPODCAST_API_TOKEN,
            ANCHOR_WEBSTATION_ID,
        )
        
        # Health check
        response = open_podcast.health()
        if response.status_code != 200:
            raise Exception(f"Open Podcast API healthcheck failed with status code {response.status_code}")
        
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
        
        logger.info(f"Successfully processed Anchor podcast {pod_name} {account_id}")
        return {
            "status": "success",
            "account_id": account_id,
            "source_name": "anchor",
            "podcast_id": source_podcast_id,
            "pod_name": pod_name,
            "endpoints_processed": len(endpoints)
        }
        
    except Exception as e:
        logger.error(f"Exception while processing Anchor podcast {pod_name}: {e}")
        raise
    finally:
        # Clean up sys.path
        if str(anchor_path) in sys.path:
            sys.path.remove(str(anchor_path))

@huey.task(retries=3, retry_delay=60)
@huey.lock_task('process-podcast-task-lock')
def process_podcast_task(account_id, source_name, source_podcast_id, source_access_keys_encrypted, pod_name):
    """
    Main task dispatcher that routes to specific connector tasks based on source_name.
    """
    logger.info(f"Dispatching task for {pod_name} {account_id} for {source_name} using podcast_id {source_podcast_id}")
    
    # Decrypt access keys
    try:
        source_access_keys = decrypt_json(source_access_keys_encrypted, OPENPODCAST_ENCRYPTION_KEY)
    except Exception as e:
        logger.error(f"Failed to decrypt keys for {pod_name} {account_id}: {e}")
        raise
    
    # Handle Podigee token refresh if needed
    if source_name == "podigee":
        if not PODIGEE_CLIENT_ID or not PODIGEE_CLIENT_SECRET:
            logger.error(f"Missing Podigee credentials for {pod_name} {account_id}")
            raise ValueError("Missing Podigee credentials")
            
        try:
            db = mysql.connector.connect(
                host=MYSQL_HOST,
                port=MYSQL_PORT,
                user=MYSQL_USER,
                passwd=MYSQL_PASSWORD,
                database=MYSQL_DATABASE,
                autocommit=True,
            )
            
            source_access_keys = handle_podigee_refresh(
                db_connection=db, 
                account_id=account_id, 
                source_name=source_name, 
                source_access_keys=source_access_keys, 
                pod_name=pod_name, 
                encryption_key=OPENPODCAST_ENCRYPTION_KEY,
                client_id=PODIGEE_CLIENT_ID,
                client_secret=PODIGEE_CLIENT_SECRET,
                redirect_uri=PODIGEE_REDIRECT_URI
            )
            
            db.close()
            
        except Exception as e:
            logger.error(f"Database error during Podigee refresh for {pod_name} {account_id}: {e}")
            raise
            
        if (not source_access_keys) or ("PODIGEE_ACCESS_TOKEN" not in source_access_keys):
            logger.error(f"Failed to refresh Podigee token for {pod_name} {account_id}")
            raise ValueError("Failed to refresh Podigee token")
    
    # Dispatch to appropriate connector task using generic DTO
    podcast = PodcastTask(account_id, source_name, source_podcast_id, source_access_keys, pod_name)
    
    if source_name == "spotify":
        return process_spotify_podcast(podcast)
    elif source_name == "podigee": 
        return process_podigee_podcast(podcast)
    elif source_name == "apple":
        return process_apple_podcast(podcast)
    elif source_name == "anchor":
        return process_anchor_podcast(podcast)
    else:
        # No subprocess fallback - all connectors must be implemented as direct tasks
        raise ValueError(f"Unsupported connector type: {source_name}. Supported types: spotify, podigee, apple, anchor")

@huey.task(expires=dt.timedelta(hours=24))
@huey.lock_task('queue-all-podcast-tasks-lock')
def queue_all_podcast_tasks():
    """
    Discover all podcast tasks from database and queue them for processing.
    """
    logger.info("Queuing all podcast tasks from database")
    
    # Connect to database
    try:
        db = mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            passwd=MYSQL_PASSWORD,
            database=MYSQL_DATABASE,
            autocommit=True,
        )
    except mysql.connector.Error as e:
        logger.error(f"Error connecting to mysql: {e}")
        raise

    # Fetch all podcast tasks
    sql = """
      SELECT account_id, source_name, source_podcast_id, source_access_keys_encrypted, pod_name
      FROM podcastSources JOIN openpodcast.podcasts USING (account_id)
    """
    
    queued_count = 0
    
    try:
        with db.cursor() as cursor:
            cursor.execute(sql)
            results = cursor.fetchall()
            
        for (account_id, source_name, source_podcast_id, source_access_keys_encrypted, pod_name) in results:
            # Queue each podcast task for processing
            task_result = process_podcast_task(
                account_id, 
                source_name, 
                source_podcast_id, 
                source_access_keys_encrypted, 
                pod_name
            )
            queued_count += 1
            logger.info(f"Queued task {queued_count} for {pod_name} {account_id} ({source_name})")
            
    finally:
        db.close()
    
    logger.info(f"Successfully queued {queued_count} podcast tasks")
    return {"queued_count": queued_count}

@huey.periodic_task(crontab(minute='0', hour='11'))
@huey.lock_task('schedule-podcast-tasks-lock')
def schedule_podcast_tasks():
    """
    Periodic task that runs once per day at 11 AM to queue all podcast tasks.
    """
    logger.info("Starting periodic podcast task scheduling")
    
    try:
        result = queue_all_podcast_tasks()
        logger.info(f"Periodic scheduler completed successfully: {result}")
        
    except Exception as e:
        logger.error(f"Periodic scheduler failed: {e}")
        # Don't raise - let the task complete and retry next hour