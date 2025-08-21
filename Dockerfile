# syntax=docker/dockerfile:1
# Pin to linux/amd64 as we deploy to x86_64 servers
FROM --platform=linux/amd64 python:3.11-slim-bullseye

# Install cron and gpg for decrypting data fetched from db
RUN apt-get update \
    && apt-get install -y cron gpg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY connector_manager/requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY connector_manager/manager manager
COPY connector_manager/run_huey_consumer.py run_huey_consumer.py
COPY spotify spotify
COPY apple apple
COPY anchor anchor
COPY podigee podigee

COPY entrypoint.sh entrypoint.sh

RUN chmod +x ./entrypoint.sh ./run_huey_consumer.py

CMD ["/app/entrypoint.sh"]