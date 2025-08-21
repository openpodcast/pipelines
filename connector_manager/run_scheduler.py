#!/usr/bin/env python3
"""
Task scheduler script that queues all podcast tasks.
This is meant to be run periodically (e.g., via cron).
"""
import sys
from pathlib import Path

# Add the manager directory to the Python path
sys.path.insert(0, str(Path(__file__).parent / "manager"))

if __name__ == "__main__":
    from manager.scheduler import main
    main()