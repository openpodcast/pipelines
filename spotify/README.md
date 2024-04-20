# Spotify Data

This uses our Spotify connector to fetch data for our podcasts from the Spotify
API. To add a new podcast, create a new workflow file in the `.github/workflows`
directory and set the required environment variables.

The Spotify connector project is located
[here](https://github.com/openpodcast/spotify-connector).

## Getting Started

### Local Development

Here is how you can run the pipeline locally:

1. Install the dependencies with `pip install -r requirements.txt`.
2. Create a `.env` file with your Spotify API credentials.
   Take a look at the `.env.sample` file for reference.
3. Run the pipeline with `make dev`.

### Docker Image

Alternatively, we provide a Docker image for the connector.

By default, the connector will fetch data from Spotify once a day.
You can set your own schedule for executing the connector like so:

```bash
docker build -t openpodcast/spotify-connector .
docker run --init -it --env-file .env -e 'CRON_SCHEDULE=00 10 * * *' openpodcast/spotify-connector
```
