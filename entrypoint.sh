#!/bin/bash

CRON_FILE=/etc/cron.d/run
mkdir -p /etc/cron.d

CRON_PATTERN=${CRON_SCHEDULE:-"0 10 * * *"}

echo -e "${CRON_PATTERN} root cd /app && python -m manager > /proc/1/fd/1 2>/proc/1/fd/2 \n" > ${CRON_FILE}

printenv | cat - ${CRON_FILE} > /tmp/cron
cat /tmp/cron > ${CRON_FILE}
chmod 0644 ${CRON_FILE}

# On startup, check if the scheduled cron time has already passed today.
# If so, run the manager once immediately to resume any unfinished work
# (e.g. after a container restart mid-day following a deployment).
# The manager's SQL query is already idempotent: it skips podcasts that
# have already been fully fetched today, so this is safe to run at any time.
CRON_MINUTE=$(echo "$CRON_PATTERN" | awk '{print $1}')
CRON_HOUR=$(echo "$CRON_PATTERN" | awk '{print $2}')

if [[ "$CRON_HOUR" =~ ^[0-9]+$ ]] && [[ "$CRON_MINUTE" =~ ^[0-9]+$ ]]; then
    # Use 10# prefix to force base-10 interpretation and avoid octal errors
    # with zero-padded values such as 08 or 09.
    CURRENT_TOTAL=$(( 10#$(date +%H) * 60 + 10#$(date +%M) ))
    CRON_TOTAL=$(( 10#$CRON_HOUR * 60 + 10#$CRON_MINUTE ))

    if [ "$CURRENT_TOTAL" -ge "$CRON_TOTAL" ]; then
        echo "Startup: scheduled time (${CRON_HOUR}:$(printf '%02d' "$CRON_MINUTE")) has passed, triggering pipeline to resume any unfinished work..."
        cd /app && python -m manager > /proc/1/fd/1 2>/proc/1/fd/2
    fi
else
    echo "Startup: cron pattern '${CRON_PATTERN}' uses non-numeric hour/minute fields; skipping startup trigger."
fi

echo "starting cron (${CRON_PATTERN})"
cron -f