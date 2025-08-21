#!/usr/bin/env python3
"""
Huey-based podcast task scheduler.
"""
from manager.load_env import load_env, load_file_or_env
from manager.tasks import queue_all_podcast_tasks
from loguru import logger
import sys

print("Initializing Huey scheduler")

# Validate required environment variables
OPENPODCAST_ENCRYPTION_KEY = load_file_or_env("OPENPODCAST_ENCRYPTION_KEY")

if not OPENPODCAST_ENCRYPTION_KEY:
    logger.error("No OPENPODCAST_ENCRYPTION_KEY found")
    exit(1)

# Check for interactive mode
interactive_mode = False
if "--interactive" in sys.argv:
    logger.warning("Interactive mode is not supported with Huey. All tasks will be queued automatically.")
    # Could implement interactive mode by filtering tasks before queuing
    
def main():
    """Queue all podcast tasks for processing by Huey workers."""
    logger.info("Starting podcast task scheduler")
    
    try:
        # Queue all tasks - this will return immediately
        result = queue_all_podcast_tasks()
        logger.info(f"Scheduler completed: {result}")
        
    except Exception as e:
        logger.error(f"Scheduler failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()