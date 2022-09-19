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


class PostHog:
    def __init__(self, api_key, feed):
        self.api_key = api_key
        self.feed = feed
        self.date = dt.datetime.now().strftime("%Y-%m-%d")

    def capture(self, event, properties):
        posthog.capture(self.feed, event, {
            'feed': self.feed,
            'source': 'Spotify Connector',
            'day': self.date,
            # Merge in properties
            **properties
        })


def main():
    connector = SpotifyConnector(
        base_url=BASE_URL,
        client_id=CLIENT_ID,
        podcast_id=PODCAST_ID,
        sp_dc=SP_DC,
        sp_key=SP_KEY,
    )
    posthog_client = PostHog(
        api_key=posthog.project_api_key,
        feed=FEED_URL,
    )
    end = dt.datetime.now()
    start = dt.datetime.now() - dt.timedelta(days=1)

    metadata = connector.metadata()
    logger.info(f"Metadata: {metadata}")
    with open(f"metadata/{dt.datetime.now()}.json", "w") as f:
        json.dump(metadata, f)

    for metric in ['totalEpisodes', 'starts',
                   'streams', 'listeners', 'followers']:
        posthog_client.capture(metric, {
            'count': metadata[metric],
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

        for metric in ['starts', 'streams']:
            posthog_client.capture(metric, {
                'count': episode[metric],
                'episode': episode['name'],
            })
        count = listeners["counts"][-1]["count"]
        posthog_client.capture("listeners", {
            count: count,
            'episode': episode['name'],
        })

        # Fetch aggregate data for podcast
        aggregate = connector.aggregate(id, start, end)
        logger.info("Podcast Aggregate = {}", json.dumps(aggregate, indent=4))
        with open(f"aggregate/{id}-{dt.datetime.now()}.json", "w") as f:
            json.dump(aggregate, f, indent=4)

        # TODO: Send aggregate data to PostHog


if __name__ == "__main__":
    main()
