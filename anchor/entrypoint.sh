#!/bin/bash

CRON_FILE=/etc/cron.d/run
mkdir -p /etc/cron.d

CRON_PATTERN=${CRON_SCHEDULE:-"0 10 * * *"}

echo -e "${CRON_PATTERN} root cd /app && python -m job > /proc/1/fd/1 2>/proc/1/fd/2 \n" > ${CRON_FILE}

printenv | cat - ${CRON_FILE} > /tmp/cron
cat /tmp/cron > ${CRON_FILE}
chmod 0644 ${CRON_FILE}

echo "starting cron (${CRON_PATTERN})"
cron -f