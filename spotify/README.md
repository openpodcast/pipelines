# Spotify Data

This uses our Spotify connector to fetch data for our podcasts from the Spotify
API.

The Spotify connector project is located
[here](https://github.com/openpodcast/spotify-connector).

## Running

In production this connector is invoked by the
[`connector_manager`](../connector_manager) as a subprocess; it is not built or
shipped as a standalone Docker image. See the top-level
[`README`](../README.md) for how to run the full stack.

For local development inside this directory:

1. Install the dependencies with `make install`.
2. Create a `.env` file with your Spotify API credentials.
   Take a look at the `.env.sample` file for reference.
3. Run the pipeline with `make dev`.
