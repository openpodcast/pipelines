FROM python:3.11-slim-bullseye

# Install cron and gpg for decrypting data fetched from db
RUN apt-get update \
    && apt-get install -y cron gpg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY connector_manager/requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY connector_manager/manager manager
COPY spotify spotify
COPY apple apple

COPY entrypoint.sh entrypoint.sh

RUN chmod +x ./entrypoint.sh

CMD ["/app/entrypoint.sh"]