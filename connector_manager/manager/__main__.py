import multiprocessing
import os
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

import mysql.connector
from loguru import logger

from manager.cryptography import decrypt_json
from manager.load_env import load_env, load_file_or_env

# Import the Podigee connector functionality
from manager.podigee_connector import handle_podigee_refresh

print("Initializing worker environment")

# Common environment variables
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
PODIGEE_REDIRECT_URI = load_env(
    "PODIGEE_REDIRECT_URI", "https://connect.openpodcast.app/auth/v1/podigee/callback"
)

if not OPENPODCAST_ENCRYPTION_KEY:
    logger.error("No OPENPODCAST_ENCRYPTION_KEY found")
    exit(1)


def ensure_db_connection():
    """
    Ensure database connection is valid, reconnect if necessary.
    Returns the database connection.
    """
    global db
    try:
        if db is None or not db.is_connected():
            logger.info("Database connection lost, reconnecting...")
            if db is not None:
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


# try to connect to mysql or exit otherwise
try:
    db = None  # Initialize as None so ensure_db_connection can handle it
    db = ensure_db_connection()
except mysql.connector.Error as e:
    logger.error("Error connecting to mysql: ", e)
    exit(1)

# if module was started with flag --interactive, every podcast has to be approved manually
interactiveMode = False
if "--interactive" in sys.argv:
    logger.info("Interactive mode enabled")
    interactiveMode = True

# Import worker functions and types from separate module for multiprocessing
from manager.worker import PodcastJob, process_source_jobs

if __name__ == "__main__":
    import importlib.metadata

    print("--- Debug Info ---")
    commit_sha = os.environ.get("COMMIT_SHA")
    if commit_sha:
        print(f"Commit ID: {commit_sha} (from env)")
    else:
        try:
            commit_id = (
                subprocess.check_output(
                    ["git", "rev-parse", "HEAD"], stderr=subprocess.DEVNULL
                )
                .decode("utf-8")
                .strip()
            )
            print(f"Commit ID: {commit_id}")
        except Exception:
            print("Commit ID: Unknown (not a git repo or git not installed)")

    for connector in [
        "appleconnector",
        "spotifyconnector",
        "anchorconnector",
        "podigeeconnector",
    ]:
        try:
            version = importlib.metadata.version(connector)
            print(f"{connector} version: {version}")
        except importlib.metadata.PackageNotFoundError:
            print(f"{connector} version: Unknown (not installed)")
    print("------------------")

    print("Fetching all podcast tasks from database...")
    sql = """
        SELECT
            account_id,
            source_name,
            source_podcast_id,
            source_access_keys_encrypted,
            pod_name
        FROM
            podcastSources
            JOIN openpodcast.podcasts USING (account_id)
    """

    with db.cursor() as cursor:
        cursor.execute(sql)
        results = cursor.fetchall()

    # Handle interactive mode by filtering jobs upfront
    jobs_to_process = []
    for row in results:
        job = PodcastJob(
            account_id=row[0],
            source_name=row[1],
            source_podcast_id=row[2],
            source_access_keys_encrypted=row[3],
            pod_name=row[4],
        )

        if interactiveMode:
            print(
                f"Fetch podcast {job.pod_name} {job.account_id} for {job.source_name} using podcast_id {job.source_podcast_id}? [y/n]"
            )
            if input() != "y":
                continue

        jobs_to_process.append(job)

    # Group jobs by source to avoid running multiple jobs for the same source in parallel
    # This prevents rate limiting and credential issues with Apple, Spotify, etc.
    jobs_by_source = defaultdict(list)
    for job in jobs_to_process:
        jobs_by_source[job.source_name].append(job)

    # Process jobs: run different sources in parallel, but same-source jobs sequentially
    if jobs_to_process:
        logger.info(
            f"Processing {len(jobs_to_process)} jobs across {len(jobs_by_source)} sources..."
        )
        print(f"Sources: {list(jobs_by_source.keys())}")
        print(f"Jobs to process: {jobs_to_process}")

        all_results = []

        # Use multiprocessing to process different sources in parallel
        with multiprocessing.Pool() as pool:
            results_by_source = pool.map(process_source_jobs, jobs_by_source.values())

        # Flatten results
        for source_results in results_by_source:
            all_results.extend(source_results)

        successful = sum(1 for r in all_results if r)
        failed = sum(1 for r in all_results if not r)

        logger.info(f"Completed. Successful: {successful}, Failed: {failed}")
    else:
        logger.info("No jobs to process")
