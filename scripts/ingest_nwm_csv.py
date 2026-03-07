#!/usr/bin/env python3
"""
NWM Velocity Ingest - CSV/COPY approach (lowest memory)
Converts NetCDF to CSV then uses PostgreSQL COPY for fast loading
"""
import os
import sys
import requests
import tempfile
import csv
from datetime import datetime, timezone
import psycopg2

DATABASE_URL = os.environ.get('DATABASE_URL', 
    'postgresql://postgres:driftingInVermont@driftwise-west.cfs02ime4lxt.us-west-2.rds.amazonaws.com:5432/gisdata')
NWM_BASE_URL = 'https://noaa-nwm-pds.s3.amazonaws.com'

def get_latest_nwm_url():
    import re
    from datetime import timedelta
    for day_offset in [0, 1]:
        check_date = (datetime.now(timezone.utc) - timedelta(days=day_offset)).strftime('%Y%m%d')
        list_url = f"{NWM_BASE_URL}/?list-type=2&prefix=nwm.{check_date}/analysis_assim/"
        resp = requests.get(list_url)
        pattern = rf'nwm\.{check_date}/analysis_assim/nwm\.t(\d{{2}})z\.analysis_assim\.channel_rt\.tm00\.conus\.nc'
        matches = re.findall(pattern, resp.text)
        if matches:
            latest_hour = max(matches)
            return f"{NWM_BASE_URL}/nwm.{check_date}/analysis_assim/nwm.t{latest_hour}z.analysis_assim.channel_rt.tm00.conus.nc", f"{check_date}T{latest_hour}:00:00Z"
    raise Exception("No NWM data found")

def main():
    start = datetime.now()
    print(f"=== NWM CSV Ingest: {start.isoformat()} ===", flush=True)
    
    url, timestamp = get_latest_nwm_url()
    print(f"Latest: {timestamp}", flush=True)
    
    # Download
    print("Downloading...", flush=True)
    resp = requests.get(url, stream=True)
    resp.raise_for_status()
    fd, nc_path = tempfile.mkstemp(suffix='.nc')
    with os.fdopen(fd, 'wb') as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)
    print(f"Downloaded: {nc_path}", flush=True)
    
    # Convert to CSV using netCDF4 with chunking
    print("Converting to CSV...", flush=True)
    import netCDF4 as nc
    import numpy as np
    
    csv_path = nc_path.replace('.nc', '.csv')
    ds = nc.Dataset(nc_path, 'r')
    
    comids = ds.variables['feature_id']
    velocities = ds.variables['velocity']
    streamflows = ds.variables['streamflow']
    total = len(comids)
    
    written = 0
    with open(csv_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        
        # Process in chunks of 100k
        chunk_size = 100000
        for start_idx in range(0, total, chunk_size):
            end_idx = min(start_idx + chunk_size, total)
            
            c_chunk = comids[start_idx:end_idx]
            v_chunk = velocities[start_idx:end_idx]
            f_chunk = streamflows[start_idx:end_idx]
            
            for i in range(len(c_chunk)):
                vel = float(v_chunk[i]) if not np.ma.is_masked(v_chunk[i]) else 0
                flow = float(f_chunk[i]) if not np.ma.is_masked(f_chunk[i]) else 0
                if vel > 0 or flow > 0:
                    writer.writerow([int(c_chunk[i]), vel, flow])
                    written += 1
            
            print(f"  Processed {end_idx:,} / {total:,}", flush=True)
    
    ds.close()
    os.unlink(nc_path)
    print(f"CSV written: {written:,} rows", flush=True)
    
    # Load via COPY
    print("Loading to database...", flush=True)
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    
    cur.execute("TRUNCATE TABLE nwm_velocity")
    conn.commit()
    
    with open(csv_path, 'r') as f:
        cur.copy_expert("COPY nwm_velocity (comid, velocity_ms, streamflow_cms) FROM STDIN WITH CSV", f)
    
    cur.execute("UPDATE nwm_velocity SET updated_at = %s", (timestamp,))
    conn.commit()
    
    cur.execute("SELECT COUNT(*) FROM nwm_velocity")
    count = cur.fetchone()[0]
    
    cur.close()
    conn.close()
    os.unlink(csv_path)
    
    elapsed = (datetime.now() - start).total_seconds()
    print(f"\n=== Done: {count:,} rows in {elapsed:.0f}s ===", flush=True)

if __name__ == '__main__':
    main()
