# Podigee Data

This uses the Podigee connector to fetch data for podcasts from the Podigee API.

The connector is available on [GitHub](https://github.com/openpodcast/podigee-connector).

## Configuration

Take a look at the [`.env.sample`](.env.sample) file to see which environment
variables you need to set.
Copy the file to `.env` and set the variables accordingly.

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
