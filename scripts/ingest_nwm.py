#!/usr/bin/env python3
"""
NWM (National Water Model) Velocity Ingest Script

Downloads the latest NWM channel_rt data from NOAA S3 and loads velocities into PostgreSQL.
Run hourly via cron for real-time velocity updates.

Data source: s3://noaa-nwm-pds/nwm.{YYYYMMDD}/analysis_assim/
"""

import os
import sys
import requests
import tempfile
from datetime import datetime, timezone
import xarray as xr
import numpy as np
import psycopg2
from psycopg2.extras import execute_values

# Configuration
DATABASE_URL = os.environ.get('DATABASE_URL', 
    'postgresql://postgres:driftingInVermont@driftwise-west.cfs02ime4lxt.us-west-2.rds.amazonaws.com:5432/gisdata')
NWM_BASE_URL = 'https://noaa-nwm-pds.s3.amazonaws.com'
BATCH_SIZE = 10000  # Insert in batches (smaller to avoid OOM)

def get_latest_nwm_url():
    """Find the latest NWM channel_rt file URL."""
    today = datetime.now(timezone.utc).strftime('%Y%m%d')
    
    # List files for today
    list_url = f"{NWM_BASE_URL}/?list-type=2&prefix=nwm.{today}/analysis_assim/&delimiter=/"
    resp = requests.get(list_url)
    
    # Find all channel_rt.tm00 files (current time)
    import re
    pattern = rf'nwm\.{today}/analysis_assim/nwm\.t(\d{{2}})z\.analysis_assim\.channel_rt\.tm00\.conus\.nc'
    matches = re.findall(pattern, resp.text)
    
    if not matches:
        # Try yesterday if today's data not available yet
        from datetime import timedelta
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime('%Y%m%d')
        list_url = f"{NWM_BASE_URL}/?list-type=2&prefix=nwm.{yesterday}/analysis_assim/"
        resp = requests.get(list_url)
        pattern = rf'nwm\.{yesterday}/analysis_assim/nwm\.t(\d{{2}})z\.analysis_assim\.channel_rt\.tm00\.conus\.nc'
        matches = re.findall(pattern, resp.text)
        today = yesterday
    
    if not matches:
        raise Exception("No NWM data found for today or yesterday")
    
    # Get the latest hour
    latest_hour = max(matches)
    url = f"{NWM_BASE_URL}/nwm.{today}/analysis_assim/nwm.t{latest_hour}z.analysis_assim.channel_rt.tm00.conus.nc"
    
    return url, f"{today}T{latest_hour}:00:00Z"


def download_nwm(url):
    """Download NWM NetCDF file to temp location."""
    print(f"Downloading: {url}")
    
    resp = requests.get(url, stream=True)
    resp.raise_for_status()
    
    # Save to temp file
    fd, path = tempfile.mkstemp(suffix='.nc')
    with os.fdopen(fd, 'wb') as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)
    
    size_mb = os.path.getsize(path) / 1024 / 1024
    print(f"Downloaded: {size_mb:.1f} MB")
    
    return path


def parse_nwm(nc_path):
    """Parse NWM NetCDF and extract velocity data."""
    print("Parsing NetCDF...")
    
    ds = xr.open_dataset(nc_path)
    
    comids = ds['feature_id'].values
    velocities = ds['velocity'].values
    streamflows = ds['streamflow'].values
    
    # Get timestamp
    time_val = ds['time'].values[0]
    timestamp = np.datetime_as_string(time_val, unit='s')
    
    ds.close()
    
    print(f"Parsed {len(comids):,} reaches")
    print(f"NWM timestamp: {timestamp}")
    
    return comids, velocities, streamflows, timestamp


def load_to_database(comids, velocities, streamflows, timestamp):
    """Load velocity data into PostgreSQL."""
    print("Loading to database...")
    
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    
    # Prepare data - only include reaches with valid velocity
    data = []
    for i in range(len(comids)):
        vel = velocities[i]
        flow = streamflows[i]
        if vel > 0 or flow > 0:  # Skip zero-flow reaches
            data.append((int(comids[i]), float(vel), float(flow)))
    
    print(f"Valid reaches with flow: {len(data):,}")
    
    # Clear old data and insert new
    cur.execute("TRUNCATE TABLE nwm_velocity")
    
    # Insert in batches
    for i in range(0, len(data), BATCH_SIZE):
        batch = data[i:i+BATCH_SIZE]
        execute_values(cur, """
            INSERT INTO nwm_velocity (comid, velocity_ms, streamflow_cms)
            VALUES %s
        """, batch)
        
        if (i + BATCH_SIZE) % 200000 == 0:
            print(f"  Inserted {min(i + BATCH_SIZE, len(data)):,} rows...")
    
    # Update timestamp for all rows
    cur.execute("UPDATE nwm_velocity SET updated_at = %s", (timestamp,))
    
    conn.commit()
    cur.close()
    conn.close()
    
    print(f"Loaded {len(data):,} velocity records")


def main():
    """Main ingest workflow."""
    start = datetime.now()
    print(f"=== NWM Velocity Ingest Started: {start.isoformat()} ===\n")
    
    try:
        # Find latest NWM file
        url, timestamp = get_latest_nwm_url()
        print(f"Latest NWM: {timestamp}\n")
        
        # Download
        nc_path = download_nwm(url)
        
        try:
            # Parse
            comids, velocities, streamflows, ts = parse_nwm(nc_path)
            
            # Load to DB
            load_to_database(comids, velocities, streamflows, ts)
            
        finally:
            # Cleanup temp file
            os.unlink(nc_path)
        
        elapsed = (datetime.now() - start).total_seconds()
        print(f"\n=== Ingest Complete: {elapsed:.1f}s ===")
        
    except Exception as e:
        print(f"\n!!! Ingest Failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
