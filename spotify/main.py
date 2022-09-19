import os
import datetime as dt
import json
from loguru import logger
from spotifyconnector import SpotifyConnector
import posthog

BASE_URL = "https://generic.wg.spotify.com/podcasters/v0"
CLIENT_ID = "05a1371ee5194c27860b3ff3ff3979d2"
PODCAST_ID = "0WgG3O6LTgbGN5SQmVrNRG"
SP_DC = os.environ.get("SPOTIFY_SP_DC")
SP_KEY = os.environ.get("SPOTIFY_SP_KEY")
FEED_URL = 'https://feeds.redcircle.com/2c2cd740-1c1f-4928-adac-98a692dbf4c2'
posthog.project_api_key = os.environ.get("PH_PROJECT_API_KEY")


def main():
    connector = SpotifyConnector(
        base_url=BASE_URL,
        client_id=CLIENT_ID,
        podcast_id=PODCAST_ID,
        sp_dc=SP_DC,
        sp_key=SP_KEY,
    )
    today = dt.datetime.now().strftime("%Y-%m-%d")
    end = dt.datetime.now()
    start = dt.datetime.now() - dt.timedelta(days=1)

    metadata = connector.metadata()
    logger.info(f"Metadata: {metadata}")
    with open(f"metadata/{dt.datetime.now()}.json", "w") as f:
        json.dump(metadata, f)

    posthog.capture(FEED_URL, 'connector', {
        'feed': FEED_URL,
        'source': 'Spotify',
        'totalEpisodes': metadata["totalEpisodes"],
        'starts': metadata["starts"],
        'streams': metadata["streams"],
        'listeners': metadata["listeners"],
        'followers': metadata["followers"],
    })

    episodes = connector.episodes(start, end)
    with open(f"episodes/{dt.datetime.now()}.json", "w") as f:
        json.dump(episodes, f, indent=4)

    for episode in episodes["episodes"]:
        id = episode['id']
        logger.info(f"Fetching data for {id}")

        streams = connector.streams(id, start, end)
        logger.info("Streams = {}", json.dumps(streams, indent=4))
        with open(f"streams/{id}-{dt.datetime.now()}.json", "w") as f:
            json.dump(streams, f, indent=4)

        # Fetch listener data for podcast
        listeners = connector.listeners(id, start, end)
        logger.info("Podcast Listeners = {}", json.dumps(listeners, indent=4))
        with open(f"listeners/{id}-{dt.datetime.now()}.json", "w") as f:
            json.dump(listeners, f, indent=4)

        count = listeners["counts"][-1]["count"]
        posthog.capture(FEED_URL, f"connector", {
            'feed': FEED_URL,
            'source': 'Spotify',
            "episode": episode['name'],
            'day': today,
            'starts': episode["starts"],
            'streams': episode["streams"],
            'listeners': count,
        })

        # Fetch aggregate data for podcast
        aggregate = connector.aggregate(id, start, end)
        logger.info("Podcast Aggregate = {}", json.dumps(aggregate, indent=4))
        with open(f"aggregate/{id}-{dt.datetime.now()}.json", "w") as f:
            json.dump(aggregate, f, indent=4)

        # TODO: Send aggregate data to PostHog


if __name__ == "__main__":
    main()
