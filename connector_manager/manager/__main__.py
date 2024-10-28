from manager.load_env import load_env
from manager.load_env import load_file_or_env
import mysql.connector
from manager.cryptography import decrypt_json
import os
import subprocess
from pathlib import Path
from loguru import logger
import sys

print("Initializing worker environment")

CONNECTORS_PATH = load_env("CONNECTORS_PATH", ".")
MYSQL_HOST = load_env("MYSQL_HOST", "localhost")
MYSQL_PORT = load_env("MYSQL_PORT", 3306)
MYSQL_USER = load_env("MYSQL_USER", "root")
MYSQL_PASSWORD = load_file_or_env("MYSQL_PASSWORD")
MYSQL_DATABASE = load_env("MYSQL_DATABASE", "openpodcast_auth")
OPENPODCAST_ENCRYPTION_KEY = load_file_or_env("OPENPODCAST_ENCRYPTION_KEY")

if not OPENPODCAST_ENCRYPTION_KEY:
    logger.error("No OPENPODCAST_ENCRYPTION_KEY found")
    exit(1)

# try to connect to mysql or exit otherwise
try:
    db = mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        passwd=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
    )
except mysql.connector.Error as e:
    logger.error("Error connecting to mysql: ", e)
    exit(1)

# if module was started with flag --interactive, every podcast has to be approved manually
interactiveMode = False
if "--interactive" in sys.argv:
    logger.info("Interactive mode enabled")
    interactiveMode = True

print("Fetching all podcast tasks from database...")
sql = """
  SELECT account_id, source_name, source_podcast_id, source_access_keys_encrypted, pod_name
  FROM podcastSources JOIN openpodcast.podcasts USING (account_id)
"""

successful = 0
failed = 0

with db.cursor() as cursor:
    cursor.execute(sql)
    results = cursor.fetchall()

for (
    account_id,
    source_name,
    source_podcast_id,
    source_access_keys_encrypted,
    pod_name,
) in results:
    if interactiveMode:
        print(
            f"Fetch podcast {pod_name} {account_id} for {source_name} using podcast_id {source_podcast_id}? [y/n]"
        )
        if input() != "y":
            continue

    # all keys that are needed to access the source
    print(f"Decrypting keys for {pod_name} {account_id} for {source_name}")
    source_access_keys = decrypt_json(
        source_access_keys_encrypted, OPENPODCAST_ENCRYPTION_KEY
    )

    logger.info(
        f"Starting fetcher for {pod_name} {account_id} for {source_name} using podcast_id {source_podcast_id}"
    )

    # parent path of fetcher/connector
    cwd = Path(CONNECTORS_PATH) / source_name
    try:
        # run an external process, switch to right fetcher depending on
        # source_name, and set env variables from source_access_keys
        result = subprocess.run(
            ["python", "-m", "job"],
            cwd=cwd,
            env={
                **os.environ,
                **source_access_keys,
                "PODCAST_ID": source_podcast_id,
                "PODCAST_NAME": pod_name,
            },
            text=True,
        )
        if result.returncode == 0:
            successful += 1
        else:
            failed += 1
            logger.error(f"Failed to fetch {pod_name}: {result.stderr}")
    except Exception as e:
        failed += 1
        logger.error(f"Exception while fetching {pod_name}: {e}")

logger.info(f"Completed. Successful: {successful}, Failed: {failed}")
