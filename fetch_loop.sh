#!/bin/sh

INTERVAL=${FETCH_INTERVAL:-300}

echo "Starting fetch loop (interval: ${INTERVAL}s)"

run_step() {
    echo "$(date): Running $1..."
    python "$1" || echo "$(date): WARNING: $1 failed with exit code $?"
}

while true; do
    echo "$(date): Starting fetch cycle..."
    run_step /app/fetch_flyover.py
    run_step /app/fetch_powpeg.py
    run_step /app/fetch_btc_locked.py
    run_step /app/fetch_route_health.py
    run_step /app/generate_report.py
    run_step /app/check_alerts.py
    echo "$(date): Fetch cycle complete. Sleeping ${INTERVAL}s..."
    sleep "$INTERVAL"
done
