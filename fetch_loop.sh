#!/bin/sh

INTERVAL=${FETCH_INTERVAL:-300}

echo "Starting fetch loop (interval: ${INTERVAL}s)"

while true; do
    echo "$(date): Starting fetch cycle..."
    python /app/fetch_flyover.py
    python /app/fetch_powpeg.py
    python /app/fetch_btc_locked.py
    python /app/generate_report.py
    python /app/check_alerts.py
    echo "$(date): Fetch cycle complete. Sleeping ${INTERVAL}s..."
    sleep "$INTERVAL"
done
