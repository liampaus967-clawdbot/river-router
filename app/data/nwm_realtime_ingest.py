#!/usr/bin/env python3
"""
NWM Real-Time Velocity Ingest Script

Fetches actual real-time streamflow and velocity data from NOAA's 
National Water Model via AWS Open Data.

Data source: s3://noaa-nwm-pds/
Documentation: https://registry.opendata.aws/nwm-archive/

NWM outputs channel routing data with:
- streamflow (m³/s) 
- velocity (m/s)
- nudge (data assimilation adjustment)

Forecast cycles: 00, 06, 12, 18 UTC (every 6 hours)
Short-range forecasts: 1-18 hours ahead
"""

import os
import sys
import tempfile
from datetime import datetime, timezone, timedelta
import psycopg2
from psycopg2.extras import execute_values

# Optional imports - will check availability
try:
    import boto3
    from botocore import UNSIGNED
    from botocore.config import Config
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

try:
    import xarray as xr
    HAS_XARRAY = True
except ImportError:
    HAS_XARRAY = False

try:
    from netCDF4 import Dataset
    import numpy as np
    HAS_NETCDF4 = True
except ImportError:
    HAS_NETCDF4 = False

# Database connection
DATABASE_URL = os.environ.get('DATABASE_URL', 
    'postgresql://driftwise:Pacific1ride@driftwise-dev.ck52oyeoe285.us-east-1.rds.amazonaws.com:5432/driftwise')

# AWS NWM bucket (public, no auth needed)
NWM_BUCKET = 'noaa-nwm-pds'

def check_dependencies():
    """Check if required packages are installed."""
    missing = []
    if not HAS_BOTO3:
        missing.append('boto3')
    if not HAS_NETCDF4 and not HAS_XARRAY:
        missing.append('netCDF4 or xarray')
    
    if missing:
        print(f"❌ Missing dependencies: {', '.join(missing)}")
        print(f"   Install with: pip install boto3 netCDF4 numpy")
        return False
    return True

def get_db_connection():
    """Get PostgreSQL connection."""
    return psycopg2.connect(DATABASE_URL)

def get_comids_from_db():
    """Get set of COMIDs from river_edges table."""
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("SELECT DISTINCT comid FROM river_edges WHERE comid IS NOT NULL")
    comids = set(row[0] for row in cur.fetchall())
    
    cur.close()
    conn.close()
    
    return comids

def get_latest_nwm_cycle():
    """
    Determine the latest available NWM forecast cycle.
    NWM runs at 00, 06, 12, 18 UTC with ~3 hour delay.
    """
    now = datetime.now(timezone.utc)
    
    # NWM data is typically available ~3 hours after cycle time
    available_time = now - timedelta(hours=3)
    
    # Round down to nearest 6-hour cycle
    cycle_hour = (available_time.hour // 6) * 6
    cycle_time = available_time.replace(hour=cycle_hour, minute=0, second=0, microsecond=0)
    
    return cycle_time

def download_nwm_file(s3_client, date_str: str, cycle: int, forecast_hour: int = 1) -> str:
    """
    Download NWM channel routing file from S3.
    
    File path format:
    nwm.{YYYYMMDD}/short_range/nwm.t{HH}z.short_range.channel_rt.f{FFF}.conus.nc
    
    Returns path to temporary file.
    """
    key = f"nwm.{date_str}/short_range/nwm.t{cycle:02d}z.short_range.channel_rt.f{forecast_hour:03d}.conus.nc"
    
    print(f"📥 Downloading: s3://{NWM_BUCKET}/{key}")
    
    # Create temp file
    tmp = tempfile.NamedTemporaryFile(suffix='.nc', delete=False)
    tmp_path = tmp.name
    tmp.close()
    
    try:
        s3_client.download_file(NWM_BUCKET, key, tmp_path)
        print(f"   Downloaded to: {tmp_path}")
        return tmp_path
    except Exception as e:
        print(f"   ❌ Download failed: {e}")
        # Try previous cycle
        raise

def extract_velocities_netcdf4(nc_path: str, comids: set) -> list:
    """Extract velocity data using netCDF4 library."""
    print(f"📊 Extracting velocities with netCDF4...")
    
    ds = Dataset(nc_path, 'r')
    
    # Get arrays
    feature_ids = ds.variables['feature_id'][:]
    velocities = ds.variables['velocity'][:]
    streamflows = ds.variables['streamflow'][:]
    
    # Get reference time
    ref_time_str = ds.variables['reference_time'].units
    
    ds.close()
    
    # Build results for matching COMIDs
    results = []
    matched = 0
    
    for idx, fid in enumerate(feature_ids):
        comid = int(fid)
        if comid in comids:
            vel = float(velocities[idx])
            flow = float(streamflows[idx])
            
            # Filter out invalid values
            if vel >= 0 and not np.isnan(vel) and vel < 100:  # reasonable velocity range
                results.append({
                    'comid': comid,
                    'velocity_ms': round(vel, 4),
                    'streamflow_cms': round(flow, 3)
                })
                matched += 1
    
    print(f"   Matched {matched:,} of {len(comids):,} COMIDs")
    
    return results

def extract_velocities_xarray(nc_path: str, comids: set) -> list:
    """Extract velocity data using xarray library."""
    print(f"📊 Extracting velocities with xarray...")
    
    ds = xr.open_dataset(nc_path)
    
    feature_ids = ds['feature_id'].values
    velocities = ds['velocity'].values
    streamflows = ds['streamflow'].values
    
    ds.close()
    
    # Build results for matching COMIDs
    results = []
    matched = 0
    
    for idx, fid in enumerate(feature_ids):
        comid = int(fid)
        if comid in comids:
            vel = float(velocities[idx])
            flow = float(streamflows[idx])
            
            if vel >= 0 and vel < 100:
                results.append({
                    'comid': comid,
                    'velocity_ms': round(vel, 4),
                    'streamflow_cms': round(flow, 3)
                })
                matched += 1
    
    print(f"   Matched {matched:,} of {len(comids):,} COMIDs")
    
    return results

def update_database(data: list, cycle_time: datetime) -> int:
    """Update nwm_velocity table with new data."""
    if not data:
        print("⚠️  No data to update")
        return 0
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Prepare values
    values = [(d['comid'], d['velocity_ms'], d['streamflow_cms'], cycle_time) for d in data]
    
    print(f"💾 Updating {len(values):,} records...")
    
    query = """
        INSERT INTO nwm_velocity (comid, velocity_ms, streamflow_cms, updated_at)
        VALUES %s
        ON CONFLICT (comid) DO UPDATE SET
            velocity_ms = EXCLUDED.velocity_ms,
            streamflow_cms = EXCLUDED.streamflow_cms,
            updated_at = EXCLUDED.updated_at
    """
    
    execute_values(cur, query, values, page_size=50000)
    conn.commit()
    
    cur.close()
    conn.close()
    
    return len(values)

def main():
    """Main ingest function."""
    print("=" * 60)
    print(f"🌊 NWM Real-Time Velocity Ingest")
    print(f"   Started: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 60)
    
    # Check dependencies
    if not check_dependencies():
        sys.exit(1)
    
    # Get latest cycle
    cycle_time = get_latest_nwm_cycle()
    date_str = cycle_time.strftime('%Y%m%d')
    cycle_hour = cycle_time.hour
    
    print(f"\n📅 Target cycle: {cycle_time.isoformat()}")
    print(f"   Date: {date_str}, Cycle: {cycle_hour:02d}z")
    
    # Get COMIDs from database
    print(f"\n🔍 Fetching COMIDs from database...")
    comids = get_comids_from_db()
    print(f"   Found {len(comids):,} unique COMIDs")
    
    # Setup S3 client (unsigned for public bucket)
    s3_client = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    
    # Download NWM file
    print(f"\n📥 Downloading NWM data...")
    nc_path = None
    
    # Try current cycle, then fall back to previous
    for offset in [0, 6, 12]:
        try:
            target_time = cycle_time - timedelta(hours=offset)
            target_date = target_time.strftime('%Y%m%d')
            target_hour = target_time.hour
            
            nc_path = download_nwm_file(s3_client, target_date, target_hour)
            cycle_time = target_time  # Update to actual cycle used
            break
        except Exception as e:
            print(f"   Cycle {target_date} {target_hour:02d}z not available, trying earlier...")
            continue
    
    if not nc_path:
        print("❌ Could not download any NWM data")
        sys.exit(1)
    
    # Extract velocities
    print(f"\n📊 Extracting velocity data...")
    try:
        if HAS_NETCDF4:
            data = extract_velocities_netcdf4(nc_path, comids)
        else:
            data = extract_velocities_xarray(nc_path, comids)
    finally:
        # Clean up temp file
        if os.path.exists(nc_path):
            os.unlink(nc_path)
    
    # Update database
    print(f"\n💾 Updating database...")
    count = update_database(data, cycle_time)
    
    print(f"\n" + "=" * 60)
    print(f"✅ Complete!")
    print(f"   Records updated: {count:,}")
    print(f"   NWM cycle: {cycle_time.isoformat()}")
    print(f"   Finished: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 60)

if __name__ == "__main__":
    main()
