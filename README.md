# 📈 Open Podcast Metrics Pipelines

This repo contains two pipelines for collecting and processing podcast metrics data
from Spotify and Apple. It used the following connector libs:

- Spotify: <https://github.com/openpodcast/spotify-connector> 
- Apple: <https://github.com/openpodcast/apple-connector> 

A pipeline consists of a docker image which uses cron to run the connector periodically
and forwards the fetched data to the Open Podcast api.

# Getting started

Have a look at <https://github.com/openpodcast/stack> to see a full stack
which also consists of the pipeline containers to fetch data from Spotify and Apple.

## Contributing

If you also want to collect metrics for your podcast,
have a look at <https://github.com/openpodcast/stack>
or reach out. We'll get you onboarded.

You can also help contribute or request new connectors.

[Open Podcast API]: https://github.com/openpodcast/api
