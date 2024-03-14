# 📈 Open Podcast Metrics Pipelines

[![OpenPodcast Banner](https://raw.githubusercontent.com/openpodcast/banner/main/openpodcast-banner.png)](https://openpodcast.app/)

This repo contains two pipelines for collecting and processing podcast metrics data
from Spotify, Apple, and Anchor. It uses the following connector libs:

- Spotify: <https://github.com/openpodcast/spotify-connector> 
- Apple: <https://github.com/openpodcast/apple-connector> 
- Anchor: <https://github.com/openpodcast/anchor-connector>

A pipeline consists of a Docker image which uses cron to run the connector periodically
and forwards the fetched data to the Open Podcast API.

# Getting started

## Starting the full stack

For local development, you can use the `docker-compose.yml` file to run the pipeline.
To start the stack, consisting of the pipeline and a sample database, run:

```
make up
```

This will start the pipeline and a sample database. The pipeline will run every
minute and fetch data from Spotify, Apple, and Anchor according to the
`podcastSources` SQL table (see `db_local_dev/schema.sql`).

## Starting the database only

Alternatively, you can also start the database only.
This is helpful if you want to run the pipeline in interactive mode

```
docker-compose up db
```

After that, you can start the `manager` container to run the pipeline:

```
# Interactive shell
docker-compose run --rm --entrypoint /bin/bash --env-file connector_manager/.env manager

# Run the pipeline in interactive mode
python -m manager --interactive
```



## Production Usage

Have a look at <https://github.com/openpodcast/stack> to see a full stack
which also consists of the pipeline containers to fetch data.

## Contributing

If you also want to collect metrics for your podcast,
have a look at <https://github.com/openpodcast/stack>
or reach out. We'll get you onboarded.

You can also help contribute or request new connectors.

[Open Podcast API]: https://github.com/openpodcast/api
