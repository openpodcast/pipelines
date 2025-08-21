"""
Async worker for processing endpoints concurrently.
Replaces threading-based worker with asyncio.
"""
import asyncio
import aiohttp
from loguru import logger
from typing import List


async def async_worker(session: aiohttp.ClientSession, endpoint, open_podcast, delay: float = 0):
    """
    Async worker that processes a single endpoint.
    
    Args:
        session: aiohttp session for making requests
        endpoint: FetchParams object containing the endpoint info
        open_podcast: OpenPodcastConnector instance
        delay: Delay between requests in seconds
    """
    try:
        # Execute the connector call
        if hasattr(endpoint, 'spotify_call'):
            data = endpoint.spotify_call()
        elif hasattr(endpoint, 'podigee_call'):
            data = endpoint.podigee_call()
        elif hasattr(endpoint, 'anchor_call'):
            data = endpoint.anchor_call()
        elif hasattr(endpoint, 'call'):
            data = endpoint.call()
        else:
            raise ValueError("Endpoint has no callable method")
        
        # Send data to OpenPodcast API
        meta = getattr(endpoint, 'meta', None)
        response = open_podcast.post(
            endpoint.openpodcast_endpoint,
            data,
            meta,
            endpoint.start_date,
            endpoint.end_date
        )
        
        logger.debug(f"Processed {endpoint.openpodcast_endpoint}: {response.status_code}")
        
        # Apply delay if specified
        if delay > 0:
            await asyncio.sleep(delay)
            
    except Exception as e:
        logger.error(f"Error processing endpoint {endpoint.openpodcast_endpoint}: {e}")
        raise


async def process_endpoints_async(endpoints: List, open_podcast, max_workers: int = 4, delay: float = 0):
    """
    Process multiple endpoints concurrently using asyncio.
    
    Args:
        endpoints: List of FetchParams objects
        open_podcast: OpenPodcastConnector instance
        max_workers: Maximum number of concurrent workers
        delay: Delay between requests in seconds
    """
    logger.info(f"Processing {len(endpoints)} endpoints with {max_workers} async workers")
    
    # Create semaphore to limit concurrent requests
    semaphore = asyncio.Semaphore(max_workers)
    
    async def bounded_worker(session, endpoint):
        async with semaphore:
            await async_worker(session, endpoint, open_podcast, delay)
    
    # Create aiohttp session
    async with aiohttp.ClientSession() as session:
        # Create tasks for all endpoints
        tasks = [bounded_worker(session, endpoint) for endpoint in endpoints]
        
        # Execute all tasks concurrently
        await asyncio.gather(*tasks, return_exceptions=True)
    
    logger.info(f"Completed processing {len(endpoints)} endpoints")


def run_async_workers(endpoints: List, open_podcast, max_workers: int = 4, delay: float = 0):
    """
    Sync wrapper to run async endpoint processing.
    
    Args:
        endpoints: List of FetchParams objects
        open_podcast: OpenPodcastConnector instance  
        max_workers: Maximum number of concurrent workers
        delay: Delay between requests in seconds
    """
    try:
        # Get or create event loop
        loop = asyncio.get_event_loop()
    except RuntimeError:
        # Create new event loop if none exists
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    
    try:
        # Run the async processing
        loop.run_until_complete(
            process_endpoints_async(endpoints, open_podcast, max_workers, delay)
        )
    finally:
        # Don't close the loop as it might be used elsewhere
        pass