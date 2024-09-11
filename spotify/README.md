# Spotify Data

This uses our Spotify connector to fetch data for our podcasts from the Spotify
API.

The Spotify connector project is located
[here](https://github.com/openpodcast/spotify-connector).

## Getting Started

### Local Development

Here is how you can run the pipeline locally:

1. Install the dependencies with `make install`.
2. Create a `.env` file with your Spotify API credentials.
   Take a look at the `.env.sample` file for reference.
3. Run the pipeline with `make run`
4. For debugging set the environment var `STORE_DATA` to `true` to store all sent data also in files