import os
import datetime as dt
import json
from loguru import logger
from spotifyconnector import SpotifyConnector

BASE_URL = os.environ.get("SPOTIFY_BASE_URL")
CLIENT_ID = os.environ.get("SPOTIFY_CLIENT_ID")
PODCAST_ID = os.environ.get("SPOTIFY_PODCAST_ID")
SP_DC = os.environ.get("SPOTIFY_SP_DC")
SP_KEY = os.environ.get("SPOTIFY_SP_KEY")

def main():
    connector = SpotifyConnector(
        base_url=BASE_URL,
        client_id=CLIENT_ID,
        podcast_id=PODCAST_ID,
        sp_dc=SP_DC,
        sp_key=SP_KEY,
    )
    end  = dt.datetime.now()
    start = dt.datetime.now() - dt.timedelta(days=1)

    metadata = connector.metadata()
    logger.info(f"Metadata: {metadata}")
    with open(f"metadata/{dt.datetime.now()}.json", "w") as f:
        json.dump(metadata, f)

    episodes = connector.episodes(start, end)
    logger.info(f"Found {len(episodes)} episodes")
    with open(f"episodes/{dt.datetime.now()}.json", "w") as f:
        json.dump(episodes, f)

    streams = connector.streams("48DAya24YOjS7Ez49JSH3y", start, end)
    logger.info(json.dumps(streams, indent=4))
    with open(f"streams/{dt.datetime.now()}.json", "w") as f:
        json.dump(streams, f)

    # Fetch listener data for podcast
    listeners = connector.listeners("48DAya24YOjS7Ez49JSH3y", start, end)
    logger.info("Podcast Listeners = {}", json.dumps(listeners, indent=4))
    with open(f"listeners/{dt.datetime.now()}.json", "w") as f:
        json.dump(listeners, f)

    # Fetch aggregate data for podcast
    aggregate  = connector.aggregate("48DAya24YOjS7Ez49JSH3y", start, end)
    logger.info("Podcast Aggregate = {}", json.dumps(aggregate, indent=4))
    with open(f"aggregate/{dt.datetime.now()}.json", "w") as f:
        json.dump(aggregate, f)

if __name__ == "__main__":
    main()