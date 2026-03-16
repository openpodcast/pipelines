# syntax=docker/dockerfile:1
# Pin to linux/amd64 as we deploy to x86_64 servers
FROM --platform=linux/amd64 python:3.11-slim-bullseye

# Install cron and git (git is required by pip to install packages from GitHub URLs)
RUN apt-get update \
    && apt-get install -y cron git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt requirements.txt
# Install the connector from GitHub (replaces the local -e path used in development),
# then install the remaining dependencies.
RUN pip3 install git+https://github.com/openpodcast/spotify-graphql-connector.git \
    && grep -v "spotify-graphql-connector\|-e " requirements.txt | pip3 install -r /dev/stdin

COPY . .
RUN chmod +x ./entrypoint.sh

CMD ["/app/entrypoint.sh"]