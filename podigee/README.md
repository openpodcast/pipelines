# Podigee Data

This uses our Podigee connector to fetch data for our podcasts from the Podigee
API. 

The connector is available on [GitHub](https://github.com/openpodcast/podigee-connector).

## Configuration

Take a look at the [`.env.sample`](.env.sample) file to see which environment
variables you need to set.  
Copy the file to `.env` and set the variables accordingly.

## Docker Image

We provide a Docker image for the connector. You can use it to run the connector
locally.

By default, the connector will fetch data from Podigee once a day.
You can set your own schedule for executing the connector like so:

```bash
docker build -t openpodcast/podigee-connector .
docker run --init -it --env-file .env -e 'CRON_SCHEDULE=00 10 * * *' openpodcast/podigee-connector
```
