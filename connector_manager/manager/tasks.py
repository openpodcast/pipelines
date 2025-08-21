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

# Add connector paths for OpenPodcastConnector import
CONNECTORS_PATH = load_env("CONNECTORS_PATH", ".")
spotify_path = Path(CONNECTORS_PATH) / "spotify"
if str(spotify_path) not in sys.path:
    sys.path.insert(0, str(spotify_path))

from job.open_podcast import OpenPodcastConnector

# Environment variables  
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

# All processors are imported from manager.processors package

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
    
    # Create podcast DTO
    podcast = PodcastTask(account_id, source_name, source_podcast_id, source_access_keys, pod_name)
    
    # Check for OpenPodcast API token
    openpodcast_api_token = podcast.openpodcast_api_token
    if not openpodcast_api_token:
        raise ValueError("Missing required OPENPODCAST_API_TOKEN")
    
    # Create OpenPodcast connector
    openpodcast_api_endpoint = os.environ.get("OPENPODCAST_API_ENDPOINT", "https://api.openpodcast.dev")
    
    # For Anchor, use webstation_id; for others use source_podcast_id
    podcast_identifier = podcast.get_access_key("ANCHOR_WEBSTATION_ID", source_podcast_id) if source_name == "anchor" else source_podcast_id
    
    open_podcast = OpenPodcastConnector(
        openpodcast_api_endpoint,
        openpodcast_api_token,
        podcast_identifier,
    )
    
    # Health check
    response = open_podcast.health()
    if response.status_code != 200:
        raise Exception(f"Open Podcast API healthcheck failed with status code {response.status_code}")
    
    # Dispatch to appropriate connector task using generic DTO
    if source_name == "spotify":
        return process_spotify_podcast(podcast, open_podcast)
    elif source_name == "podigee": 
        return process_podigee_podcast(podcast, open_podcast)
    elif source_name == "apple":
        return process_apple_podcast(podcast, open_podcast)
    elif source_name == "anchor":
        return process_anchor_podcast(podcast, open_podcast)
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