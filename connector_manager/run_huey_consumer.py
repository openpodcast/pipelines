#!/usr/bin/env python3
"""
Huey consumer startup script.
This starts the Huey worker processes to consume tasks.
"""
import sys
import os
from pathlib import Path

# Add the manager directory to the Python path
sys.path.insert(0, str(Path(__file__).parent / "manager"))

# Import the huey instance - this will trigger task registration
from manager.huey_config import huey

if __name__ == "__main__":
    # Import tasks to register them with huey
    import manager.tasks
    
    print(f"Starting Huey consumer for {huey.name}. Storage location: `{huey.storage}`")
    
    # Start the consumer - this will block and process tasks
    from huey.consumer import Consumer
    
    consumer = Consumer(
        huey,
        workers=int(os.getenv("HUEY_WORKERS", 4)),  # Default to 4 workers
        worker_type="process",  # Use processes for better isolation and CPU utilization
        check_worker_health=True,
        health_check_interval=60,
    )
    
    try:
        consumer.run()
    except KeyboardInterrupt:
        print("\nShutting down Huey consumer...")
        consumer.stop()
    except Exception as e:
        print(f"Consumer error: {e}")
        sys.exit(1)