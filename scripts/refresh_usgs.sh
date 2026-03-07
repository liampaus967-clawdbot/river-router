#!/bin/bash
# USGS Gauge Data Refresh
# Runs every 15 minutes via cron

cd /home/ubuntu/river-router-api
python3 scripts/usgs_gauges.py fetch >> logs/usgs_refresh.log 2>&1
echo "$(date): Refresh complete" >> logs/usgs_refresh.log
