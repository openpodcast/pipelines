"""
Worker functions for multiprocessing.

These functions must be in a separate module (not __main__.py)
to be picklable for multiprocessing.Pool.
"""

from dataclasses import dataclass
from manager.load_env import load_env, load_file_or_env
import mysql.connector
from manager.cryptography import decrypt_json
import os
import subprocess
from pathlib import Path
from loguru import logger


@dataclass
class PodcastJob:
    account_id: str
    source_name: str
    source_podcast_id: str
    source_access_keys_encrypted: str
    pod_name: str

# Load environment variables
CONNECTORS_PATH = load_env("CONNECTORS_PATH", ".")
MYSQL_HOST = load_env("MYSQL_HOST", "localhost")
MYSQL_PORT = load_env("MYSQL_PORT", 3306)
MYSQL_USER = load_env("MYSQL_USER", "root")
MYSQL_PASSWORD = load_file_or_env("MYSQL_PASSWORD")
MYSQL_DATABASE = load_env("MYSQL_DATABASE", "openpodcast_auth")
OPENPODCAST_ENCRYPTION_KEY = load_file_or_env("OPENPODCAST_ENCRYPTION_KEY")

PODIGEE_CLIENT_ID = load_env("PODIGEE_CLIENT_ID")
PODIGEE_CLIENT_SECRET = load_file_or_env("PODIGEE_CLIENT_SECRET")
PODIGEE_REDIRECT_URI = load_env("PODIGEE_REDIRECT_URI",
                               "https://connect.openpodcast.app/auth/v1/podigee/callback")


def ensure_db_connection():
    """
    Ensure database connection is valid, reconnect if necessary.
    Returns the database connection.
    """
    global db
    try:
        if db is None:
            logger.info("Establishing database connection...")
            db = mysql.connector.connect(
                host=MYSQL_HOST,
                port=MYSQL_PORT,
                user=MYSQL_USER,
                passwd=MYSQL_PASSWORD,
                database=MYSQL_DATABASE,
                autocommit=True,
            )
            logger.info("Database connection established")
        elif not db.is_connected():
            logger.info("Database connection lost, reconnecting...")
            db.close()
            db = mysql.connector.connect(
                host=MYSQL_HOST,
                port=MYSQL_PORT,
                user=MYSQL_USER,
                passwd=MYSQL_PASSWORD,
                database=MYSQL_DATABASE,
                autocommit=True,
            )
            logger.info("Database connection re-established")
        return db
    except mysql.connector.Error as e:
        logger.error(f"Error connecting to mysql: {e}")
        raise


# Initialize as None for worker processes
db = None


def process_podcast_job(job):
    """
    Worker function to process a single podcast job.
    Each worker process will have its own database connection.
    """
    from manager.podigee_connector import handle_podigee_refresh

    # Each worker process needs its own database connection
    global db
    db = None

    try:
        # all keys that are needed to access the source
        print(f"Decrypting keys for {job.pod_name} {job.account_id} for {job.source_name}")
        source_access_keys = decrypt_json(
            job.source_access_keys_encrypted, OPENPODCAST_ENCRYPTION_KEY
        )

        # Handle Podigee token refresh if this is a Podigee source
        if job.source_name == "podigee":
            # check if all relevant variables are set, otherwise skip this source
            if not PODIGEE_CLIENT_ID or not PODIGEE_CLIENT_SECRET:
                logger.error(
                    f"Missing Podigee credentials for {job.pod_name} {job.account_id}. Skipping this source."
                )
                return False

            # Ensure database connection is valid before token refresh
            try:
                db = ensure_db_connection()
            except mysql.connector.Error:
                logger.error(f"Cannot establish database connection for Podigee token refresh of {job.pod_name} {job.account_id}. Skipping this source.")
                return False

            # Handle the token refresh and database update
            source_access_keys = handle_podigee_refresh(
                db_connection=db,
                account_id=job.account_id,
                source_name=job.source_name,
                source_access_keys=source_access_keys,
                pod_name=job.pod_name,
                encryption_key=OPENPODCAST_ENCRYPTION_KEY,
                client_id=PODIGEE_CLIENT_ID,
                client_secret=PODIGEE_CLIENT_SECRET,
                redirect_uri=PODIGEE_REDIRECT_URI
            )

            if (not source_access_keys) or ("PODIGEE_ACCESS_TOKEN" not in source_access_keys):
                logger.error(f"Failed to refresh Podigee token for {job.pod_name} {job.account_id}. Skipping this source.")
                return False

        logger.info(
            f"Starting fetcher for {job.pod_name} {job.account_id} for {job.source_name} using podcast_id {job.source_podcast_id}"
        )

        # parent path of fetcher/connector
        cwd = Path(CONNECTORS_PATH) / job.source_name

        # Ensure that environment variables are proper strings
        source_access_keys = {k: str(v) for k, v in source_access_keys.items()}

        # run an external process, switch to right fetcher depending on
        # source_name, and set env variables from source_access_keys
        result = subprocess.run(
            ["python", "-m", "job"],
            cwd=cwd,
            env={
                **os.environ,
                **source_access_keys,
                "PODCAST_ID": job.source_podcast_id,
                "PODCAST_NAME": job.pod_name,
            },
            text=True,
            timeout=7200,  # 120 minute timeout to prevent hanging of subprocesses
        )

        if result.returncode == 0:
            return True
        else:
            logger.error(f"Fetching of {job.pod_name} not successful. Subprocess error output: {result.stderr}")
            return False

    except subprocess.TimeoutExpired:
        logger.error(f"Error: Timeout while fetching {job.pod_name} (exceeded 120 minutes)")
        return False
    except Exception as e:
        logger.error(f"Exception while fetching {job.pod_name}: {e}")
        return False
    finally:
        # Clean up database connection for this worker
        if db is not None and db.is_connected():
            db.close()


def process_source_jobs(source_jobs):
    """
    Process all jobs for a single source sequentially.
    """
    source_results = []
    for job in source_jobs:
        result = process_podcast_job(job)
        source_results.append(result)
    return source_results
