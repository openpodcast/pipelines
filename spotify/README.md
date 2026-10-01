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

## Local development

This pipeline uses [uv](https://docs.astral.sh/uv/) for dependency and
virtualenv management. Install uv first (e.g. `brew install uv`), then from
this directory:

```bash
make install   # creates .venv and installs runtime + dev deps from uv.lock
make dev       # runs `python -m job` against values from ./.env
make test      # runs pytest
make lint      # runs ruff check + format check
```

The dependency manifest lives in [`pyproject.toml`](./pyproject.toml) and is
locked in [`uv.lock`](./uv.lock); both files should be committed. To add a
new runtime dependency:

```bash
uv add <package>
```

To add a new dev-only dependency (linters, test helpers, ...):

```bash
uv add --dev <package>
```

To bump the upstream Spotify connector to a new release:

```bash
uv lock --upgrade-package spotifyconnector
```

## Ingestion failures

Tasks continue after a fetch or ingestion error, but the process exits with status
1 if any task failed. Check the endpoint, date range, and episode in the failure
log before rerunning the affected range. Worker errors no longer leave the task
queue waiting indefinitely.

Date-keyed ingestion POSTs (`listeners`, `detailedStreams`, `followers`, `aggregate`,
and total/faceted/daily impressions) make at most three attempts for connection/time-out failures and
HTTP 429, 500, 502, 503, or 504. Backoff is 1 then 2 seconds; `Retry-After` can extend
it to at most 60 seconds per retry. Other non-200 responses fail immediately.
The payload is reused across attempts. Snapshot endpoints (`metadata`,
`episodeMetadata`, `performance`, and `impressions_funnel`) and unknown endpoints
are not automatically retried: their handlers may assign a server-current date,
so replaying across midnight is not guaranteed to replace the same row. Failures
still propagate. Response bodies and authorization headers are not logged.

An empty or missing show-level listener `counts` series fails the task instead of
being accepted as a successful no-op. Explicit daily counts of zero remain valid.
No missing days are synthesized, and this does not verify that every requested day
is present in a nonempty response. Episode-level empty series keep their existing
behavior. The missing-row alert remains unchanged; delayed source data can still
require a later rerun.
