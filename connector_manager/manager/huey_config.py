from huey import SqliteHuey
from manager.load_env import load_env
import logging
from pathlib import Path

# Configure Huey with SQLite
HUEY_DB_PATH = load_env("HUEY_DB_PATH", "/app/data/huey.db")

# Ensure the directory exists
Path(HUEY_DB_PATH).parent.mkdir(parents=True, exist_ok=True)

huey = SqliteHuey(
    filename=HUEY_DB_PATH,
    name='openpodcast-connector-manager',
    results=True,  # Store task results
    store_none=False,  # Don't store None results
    utc=True,  # Use UTC for all times
)

# Configure logging for Huey
logging.getLogger('huey').setLevel(logging.INFO)