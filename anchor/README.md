# Anchor Data

This uses our Anchor connector to fetch data for our podcasts from the Anchor
API. To add a new podcast, create a new workflow file in the `.github/workflows`
directory and set the required environment variables.

The connector is available on [GitHub](https://github.com/openpodcast/anchor-connector).

## Configuration

Take a look at the [`.env.sample`](.env.sample) file to see which environment
variables you need to set.  
Copy the file to `.env` and set the variables accordingly.

One peculiarity of the API is that the podcast ID is
called `webstation_id`.

## Docker Image

We provide a Docker image for the connector. You can use it to run the connector locally.

By default, the connector will fetch data from Anchor once a day.
You can set your own schedule for executing the connector like so:

```bash
docker build -t openpodcast/anchor-connector .
docker run --init -it --env-file .env -e 'CRON_SCHEDULE=00 10 * * *' openpodcast/anchor-connector
```
