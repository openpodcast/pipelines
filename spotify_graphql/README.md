# Spotify GraphQL Data

This pipeline fetches analytics data for all podcasts on a Spotify Creators account
using the [spotify-graphql-connector](https://github.com/openpodcast/spotify-graphql-connector),
which talks directly to the Spotify Creators GraphQL API.

It replaces the old [spotify-connector](https://github.com/openpodcast/spotify-connector)
pipeline, which used a REST API that Spotify has since deprecated.

## What it fetches

For every show on the account it collects:

- Plays and streams time series
- Geographic breakdown
- Age and gender demographics
- App and device breakdown
- Impressions trend and sources
- Audience discovery funnel
- All-time top episodes

For every episode it collects:

- Metadata
- Performance curve (all-time retention)
- Streams and downloads
- Daily plays
- Consumption data (all-time)
- Audience size (all-time)

No show ID needs to be configured — the pipeline discovers all shows automatically.

## Getting Started

### Local Development

1. Install dependencies:
   ```bash
   make install
   ```
2. Copy `.env.sample` to `.env` and fill in your credentials:
   ```bash
   cp .env.sample .env
   ```
3. Run the pipeline:
   ```bash
   make run
   ```

To debug without hitting the Open Podcast API, set `STORE_DATA=true` — raw JSON
responses will be written to `./data/` instead:

```bash
STORE_DATA=true make run
```

### Docker Image

A Docker image is provided for production use. By default the pipeline runs once a
day at 10:00 UTC. You can override the schedule with `CRON_SCHEDULE`:

```bash
docker build -t openpodcast/spotify-graphql-connector .
docker run --init -it --env-file .env -e 'CRON_SCHEDULE=0 10 * * *' openpodcast/spotify-graphql-connector
```

## Credentials

You need two session cookies from an active Spotify account: `sp_dc` and `sp_key`.

1. Log in to [https://creators.spotify.com](https://creators.spotify.com).
2. Open DevTools → Application → Cookies → `https://accounts.spotify.com`.
3. Copy the values of `sp_dc` and `sp_key`.

These cookies are typically valid for several months. When they expire the pipeline
will exit with a `CredentialsExpired` error — grab fresh values from your browser and
update the environment variables.

## Environment Variables

| Variable                   | Required | Default                       | Description                            |
|----------------------------|----------|-------------------------------|----------------------------------------|
| `SPOTIFY_SP_DC`            | Yes      | -                             | `sp_dc` cookie from a Spotify session  |
| `SPOTIFY_SP_KEY`           | Yes      | -                             | `sp_key` cookie from the same session  |
| `OPENPODCAST_API_TOKEN`    | Yes      | -                             | Bearer token for the Open Podcast API  |
| `OPENPODCAST_API_ENDPOINT` | No       | `https://api.openpodcast.dev` | Open Podcast API base URL              |
| `DATE_RANGE_WINDOW`        | No       | `WINDOW_LAST_SEVEN_DAYS`      | Analytics time window                  |
| `NUM_WORKERS`              | No       | `1`                           | Parallel worker threads                |
| `TASK_DELAY`               | No       | `1.0`                         | Seconds between requests per worker    |
| `STORE_DATA`               | No       | `false`                       | Write raw JSON to `./data/` locally    |
| `CRON_SCHEDULE`            | No       | `0 10 * * *`                  | Cron expression for the Docker image   |