# Spotify Data

This uses our Spotify connector to fetch data for our podcasts from the Spotify
API. To add a new podcast, create a new workflow file in the `.github/workflows`
directory and set the required environment variables.

The connector is available on [GitHub](https://github.com/openpodcast/spotify-connector).

## Docker Image

We provide a Docker image for the connector. You can use it to run the connector locally.

By default, the connector will fetch data from Spotify once a day.
You can set your own schedule for executing the connector like so:

```bash
docker build -t openpodcast/spotify-connector .
docker run --init -it --env-file .env -e 'CRON_SCHEDULE=00 10 * * *' openpodcast/spotify-connector
```
