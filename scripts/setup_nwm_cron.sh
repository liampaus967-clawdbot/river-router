#!/bin/bash
# Setup hourly NWM velocity ingest cron job

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV_PATH="$(dirname "$SCRIPT_DIR")/venv"
SCRIPT_PATH="$SCRIPT_DIR/ingest_nwm.py"
LOG_PATH="$(dirname "$SCRIPT_DIR")/logs/nwm_ingest.log"

# Create logs directory
mkdir -p "$(dirname "$LOG_PATH")"

# Create cron entry
CRON_CMD="0 * * * * cd $(dirname "$SCRIPT_DIR") && $VENV_PATH/bin/python $SCRIPT_PATH >> $LOG_PATH 2>&1"

# Check if cron already exists
if crontab -l 2>/dev/null | grep -q "ingest_nwm.py"; then
    echo "NWM cron job already exists"
    crontab -l | grep ingest_nwm
else
    # Add to crontab
    (crontab -l 2>/dev/null; echo "$CRON_CMD") | crontab -
    echo "Added NWM cron job (runs at the top of every hour)"
    echo "Cron entry: $CRON_CMD"
fi

echo ""
echo "Log file: $LOG_PATH"
echo "To check status: tail -f $LOG_PATH"
echo "To remove: crontab -e (and delete the ingest_nwm.py line)"
