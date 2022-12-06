FROM python:3.11-slim-bullseye

# Install cron
RUN apt-get update \
    && apt-get install -y cron \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY . .
RUN chmod +x ./entrypoint.sh

CMD ["/app/entrypoint.sh"]