import os
import datetime as dt
import json
from loguru import logger
from spotifyconnector import SpotifyConnector
import posthog
import requests

BASE_URL = "https://generic.wg.spotify.com/podcasters/v0"
CLIENT_ID = "05a1371ee5194c27860b3ff3ff3979d2"
PODCAST_ID = os.environ.get("PODCAST_ID")
SP_DC = os.environ.get("SPOTIFY_SP_DC")
SP_KEY = os.environ.get("SPOTIFY_SP_KEY")
FEED_URL = 'https://feeds.redcircle.com/2c2cd740-1c1f-4928-adac-98a692dbf4c2'
posthog.project_api_key = os.environ.get("PH_PROJECT_API_KEY")
OPENPODCAST_API_ENDPOINT = "https://api.openpodcast.dev/connector"
# OPENPODCAST_API_ENDPOINT = "http://localhost:8080/connector"
OPENPODCAST_API_TOKEN = os.environ.get("OPENPODCAST_API_TOKEN")

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

class OpenPodcastApi:
    def __init__(self, endpoint, token):
        self.endpoint = endpoint
        self.token = token
        pass

    def capture(self, data, range, meta = {}):
        """ 
        Send POST request to Open Podcast API.
        """
        headers = {
            'Authorization': f'Bearer {self.token}'
        }
        json = {
            "provider": "spotify",
            "version": 1,
            "retrieved": dt.datetime.now().isoformat(),
            "meta": meta,
            "range": range,
            "data": data,
        }
        print(json)
        return requests.post(self.endpoint, headers=headers, json=json)
        

def main():
    connector = SpotifyConnector(
        base_url=BASE_URL,
        client_id=CLIENT_ID,
        podcast_id=PODCAST_ID,
        sp_dc=SP_DC,
        sp_key=SP_KEY,
    )
    # posthog_client = PostHog(
    #     api_key=posthog.project_api_key,
    #     feed=FEED_URL,
    # )
    open_podcast_api = OpenPodcastApi(
        endpoint=OPENPODCAST_API_ENDPOINT,
        token=OPENPODCAST_API_TOKEN,
    )


    metadata = connector.metadata()
    logger.info(f"Metadata: {metadata}")
    with open(f"metadata/{dt.datetime.now()}.json", "w") as f:
        json.dump(metadata, f)

    start = dt.datetime.now() - dt.timedelta(days=1)
    end = dt.datetime.now()
    open_podcast_api.capture(metadata, 
        meta = {
            "show": PODCAST_ID,
            "endpoint": "metadata", 
        },
        range = {
            "start": start.strftime("%Y-%m-%d"),
            "end": end.strftime("%Y-%m-%d"),
        },
    )

    # for metric in ['totalEpisodes', 'starts',
    #                'streams', 'listeners', 'followers']:
    #     posthog_client.capture(metric, {
    #         'count': metadata[metric],
    #     })

    episodes = connector.episodes(start, end)
    with open(f"episodes/{dt.datetime.now()}.json", "w") as f:
        json.dump(episodes, f, indent=4)

    open_podcast_api.capture(episodes, 
        range = {
            "start": start.strftime("%Y-%m-%d"),
            "end": end.strftime("%Y-%m-%d"),
        },
        meta = {
            "show": PODCAST_ID,
            "endpoint": "episodes", 
        }
    )

    for episode in episodes["episodes"]:
        id = episode['id']
        logger.info(f"Fetching data for {id}")

        # TODO: Get episode metadata
        # episode_metadata = connector.episode_metadata(id)
        # if episode['starts'] == 0 and episode['streams'] == 0 and episode['listeners'] == 0:
        #     logger.info(f"Skipping {id} because it has 0 starts, streams, and listeners")
        #     continue

        streams = connector.streams(id, start, end)
        logger.info("Streams = {}", json.dumps(streams, indent=4))
        with open(f"streams/{id}-{dt.datetime.now()}.json", "w") as f:
            json.dump(streams, f, indent=4)

        open_podcast_api.capture(streams, 
            range = {
                "start": start.strftime("%Y-%m-%d"),
                "end": end.strftime("%Y-%m-%d"),
            },
            meta = {
                "show": PODCAST_ID,
                "episode": id,
                "endpoint": "detailedStreams", 
            }
        )

        # Fetch listener data for podcast
        listeners = connector.listeners(id, start, end)
        logger.info("Podcast Listeners = {}", json.dumps(listeners, indent=4))
        with open(f"listeners/{id}-{dt.datetime.now()}.json", "w") as f:
            json.dump(listeners, f, indent=4)

        open_podcast_api.capture(listeners, 
            range = {
                "start": start.strftime("%Y-%m-%d"),
                "end": end.strftime("%Y-%m-%d"),
            },
            meta = {
                "show": PODCAST_ID,
                "episode": id,
                "endpoint": "listeners", 
            }
        )

        try:
            # Fetch performance data for podcast
            performance = connector.performance(id)
            logger.info("Podcast Performance = {}", json.dumps(performance, indent=4))
            with open(f"performance/{id}-{dt.datetime.now()}.json", "w") as f:
                json.dump(performance, f, indent=4)

            open_podcast_api.capture(performance, 
                range = {
                    "start": start.strftime("%Y-%m-%d"),
                    "end": end.strftime("%Y-%m-%d"),
                },
                meta = {
                    "show": PODCAST_ID,
                    "episode": id,
                    "endpoint": "performance", 
                }
            )
        except Exception as e:
            logger.error("Failed to fetch performance data for episode {}: {}", id, e)

        # for metric in ['starts', 'streams']:
        #     posthog_client.capture(metric, {
        #         'count': episode[metric],
        #         'episode': episode['name'],
        #     })
        # count = listeners["counts"][-1]["count"]
        # posthog_client.capture("listeners", {
        #     count: count,
        #     'episode': episode['name'],
        # })

        # Fetch aggregate data for podcast
        aggregate = connector.aggregate(id, start, end)
        logger.info("Podcast Aggregate = {}", json.dumps(aggregate, indent=4))
        with open(f"aggregate/{id}-{dt.datetime.now()}.json", "w") as f:
            json.dump(aggregate, f, indent=4)

        open_podcast_api.capture(aggregate,
            range = {
                "start": start.strftime("%Y-%m-%d"),
                "end": end.strftime("%Y-%m-%d"),
            },
            meta = {
                "show": PODCAST_ID,
                "episode": id,
                "endpoint": "aggregate", 
            }
        )

        # for metric in ['listeners', 'followers']:
        #     posthog_client.capture(metric, {
        #         'count': aggregate[metric],
        #         'episode': episode['name'],
        #     })
        # TODO: Send aggregate data to PostHog


if __name__ == "__main__":
    main()
