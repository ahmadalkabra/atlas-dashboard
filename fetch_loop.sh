#!/bin/sh

while true; do
    echo "$(date): Running fetch scripts..."
    python /app/fetch_flyover.py
    python /app/fetch_powpeg.py
    python /app/fetch_btc_locked.py
    echo "$(date): Fetch complete. Sleeping for 5 minutes..."
    sleep 20
done
