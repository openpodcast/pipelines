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
COPY connector_manager/entrypoint.py entrypoint.py
COPY spotify spotify
COPY apple apple
COPY anchor anchor
COPY podigee podigee

RUN chmod +x ./entrypoint.py

CMD ["python", "/app/entrypoint.py"]