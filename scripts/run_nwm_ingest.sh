#!/bin/bash
# NWM Velocity Ingest Wrapper Script
# Runs daily at noon EST (17:00 UTC)

set -e

LOG_DIR="/home/ubuntu/river-router-api/logs"
LOG_FILE="$LOG_DIR/nwm_ingest_$(date +%Y%m%d).log"

# Create logs directory if it doesn't exist
mkdir -p "$LOG_DIR"

echo "=== NWM Ingest Starting: $(date -Iseconds) ===" >> "$LOG_FILE"

# Activate venv and run script
cd /home/ubuntu/river-router-api
source venv/bin/activate

python scripts/ingest_nwm.py >> "$LOG_FILE" 2>&1

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo "=== NWM Ingest Completed Successfully ===" >> "$LOG_FILE"
    
    # Clear API cache after successful upload to prevent disk bloat
    CACHE_FILE="/home/ubuntu/river-router-api/cache/aiohttp_cache.sqlite"
    if [ -f "$CACHE_FILE" ]; then
        rm -f "$CACHE_FILE"
        echo "=== Cleared API cache ===" >> "$LOG_FILE"
    fi
else
    echo "!!! NWM Ingest FAILED with exit code $EXIT_CODE" >> "$LOG_FILE"
fi

# Clean up logs older than 30 days
find "$LOG_DIR" -name "nwm_ingest_*.log" -mtime +30 -delete 2>/dev/null || true

exit $EXIT_CODE
