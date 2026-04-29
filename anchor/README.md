# Anchor Data

This uses our Anchor connector to fetch data for our podcasts from the Anchor
API.

The connector is available on [GitHub](https://github.com/openpodcast/anchor-connector).

## Configuration

Take a look at the [`.env.sample`](.env.sample) file to see which environment
variables you need to set.
Copy the file to `.env` and set the variables accordingly.

One peculiarity of the API is that the podcast ID is
called `webstation_id`.

## Running

In production this connector is invoked by the
[`connector_manager`](../connector_manager) as a subprocess; it is not built or
shipped as a standalone Docker image. See the top-level
[`README`](../README.md) for how to run the full stack.

For local development inside this directory:

```bash
make install
make dev
```
