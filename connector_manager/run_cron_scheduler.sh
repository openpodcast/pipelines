#!/bin/bash
# Install crontab and start cron daemon for scheduler

# Install the crontab
crontab /app/crontab

# Create log file
touch /var/log/scheduler.log

# Start cron in foreground
exec cron -f