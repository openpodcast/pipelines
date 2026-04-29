# Apple Data

This uses our Apple connector to fetch data for our podcasts from the Apple
API.

The connector is available on [GitHub](https://github.com/openpodcast/apple-connector).

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
