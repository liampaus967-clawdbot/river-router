#!/usr/bin/env python3
"""
NWM Retrospective Data ETL
==========================
Fetches historical streamflow/velocity from NOAA's NWM v3.0 Retrospective dataset
and loads daily averages into PostgreSQL for percentile computation.

Data source: s3://noaa-nwm-retrospective-3-0-pds/CONUS/zarr/chrtout.zarr/

Usage:
    python fetch_retrospective.py --years 10 --state VT
    python fetch_retrospective.py --years 10 --comids comids.txt
    python fetch_retrospective.py --test  # Small test with 100 comids, 30 days

Requires .env file with:
    DB_HOST=...
    DB_NAME=...
    DB_USER=...
    DB_PASSWORD=...
    DB_PORT=5432
"""

import argparse
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import numpy as np
import psycopg2
from psycopg2.extras import execute_values
import zarr
import s3fs

# Load .env file
env_path = Path(__file__).parent / '.env'
if env_path.exists():
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                os.environ.setdefault(key.strip(), value.strip())

# Database connection from environment
DB_CONFIG = {
    'host': os.environ.get('DB_HOST'),
    'database': os.environ.get('DB_NAME'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'port': int(os.environ.get('DB_PORT', 5432))
}

# S3 Zarr store path
ZARR_PATH = 's3://noaa-nwm-retrospective-3-0-pds/CONUS/zarr/chrtout.zarr'

# Scale factors for packed integers
STREAMFLOW_SCALE = 0.01  # int32 -> m³/s
VELOCITY_SCALE = 0.01    # int32 -> m/s
FILL_VALUE = -999900


def get_vermont_comids(conn) -> np.ndarray:
    """Get Vermont river comids from river_velocities table."""
    with conn.cursor() as cur:
        # Vermont bounding box
        cur.execute("""
            SELECT DISTINCT comid 
            FROM river_velocities 
            WHERE ST_Intersects(
                geom, 
                ST_MakeEnvelope(-73.5, 42.7, -71.5, 45.1, 4326)
            )
            ORDER BY comid
        """)
        comids = np.array([row[0] for row in cur.fetchall()], dtype=np.int64)
    print(f"Found {len(comids):,} Vermont comids")
    return comids


def get_state_comids(conn, state: str) -> np.ndarray:
    """Get comids for a state using state boundary."""
    # For now, just support Vermont with hardcoded bbox
    if state.upper() == 'VT':
        return get_vermont_comids(conn)
    else:
        raise NotImplementedError(f"State {state} not yet supported. Add bounding box.")


def load_comids_from_file(filepath: str) -> np.ndarray:
    """Load comids from a text file (one per line)."""
    comids = []
    with open(filepath) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                comids.append(int(line))
    return np.array(comids, dtype=np.int64)


def open_zarr_store():
    """Open the NWM retrospective Zarr store from S3."""
    s3 = s3fs.S3FileSystem(anon=True)
    store = s3fs.S3Map(root=ZARR_PATH, s3=s3)
    root = zarr.open(store, mode='r')
    return root


def find_comid_indices(zarr_root, target_comids: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """
    Find the indices of target comids in the Zarr feature_id array.
    Returns (indices, found_comids) - only includes comids that exist in the dataset.
    """
    print("Loading feature_id array from Zarr...")
    feature_ids = zarr_root['feature_id'][:]
    print(f"  Total features in dataset: {len(feature_ids):,}")
    
    # Create a lookup dict for fast matching
    feature_id_to_idx = {fid: idx for idx, fid in enumerate(feature_ids)}
    
    # Find indices for our target comids
    indices = []
    found_comids = []
    missing = 0
    
    for comid in target_comids:
        if comid in feature_id_to_idx:
            indices.append(feature_id_to_idx[comid])
            found_comids.append(comid)
        else:
            missing += 1
    
    if missing > 0:
        print(f"  Warning: {missing} comids not found in NWM dataset")
    
    print(f"  Matched {len(indices):,} comids")
    return np.array(indices), np.array(found_comids)


def get_time_range(zarr_root, years: int) -> tuple[int, int, np.ndarray]:
    """
    Get time slice indices for the last N years of data.
    Returns (start_idx, end_idx, dates_array).
    """
    print("Loading time array from Zarr...")
    time_raw = zarr_root['time'][:]
    
    # Convert "hours since 1979-02-01T01:00:00" to datetime
    base_time = datetime(1979, 2, 1, 1, 0, 0)
    
    # Find the last timestamp
    last_hours = int(time_raw[-1])
    last_date = base_time + timedelta(hours=last_hours)
    print(f"  Dataset ends: {last_date.strftime('%Y-%m-%d')}")
    
    # Calculate start date (N years back)
    # For fractional years (test mode), use days instead
    if years < 1:
        days_back = int(years * 365)
        start_date = last_date - timedelta(days=days_back)
    else:
        start_date = last_date.replace(year=last_date.year - int(years))
    start_hours = (start_date - base_time).total_seconds() / 3600
    
    # Find the index where our date range starts
    start_idx = np.searchsorted(time_raw, start_hours)
    end_idx = len(time_raw)
    
    print(f"  Extracting {years} years: {start_date.strftime('%Y-%m-%d')} to {last_date.strftime('%Y-%m-%d')}")
    print(f"  Time indices: {start_idx:,} to {end_idx:,} ({end_idx - start_idx:,} hourly steps)")
    
    # Convert to actual dates for the slice
    dates = []
    for h in time_raw[start_idx:end_idx]:
        dt = base_time + timedelta(hours=int(h))
        dates.append(dt)
    
    return start_idx, end_idx, np.array(dates)


def compute_daily_averages(
    streamflow_hourly: np.ndarray,
    velocity_hourly: np.ndarray,
    dates: np.ndarray
) -> tuple[list, list, list]:
    """
    Compute daily averages from hourly data.
    Returns (unique_dates, daily_flow, daily_velocity) with shape (n_days, n_comids).
    """
    # Get unique dates (ignore time component)
    date_only = np.array([d.date() for d in dates])
    unique_dates = np.unique(date_only)
    
    n_days = len(unique_dates)
    n_comids = streamflow_hourly.shape[1]
    
    daily_flow = np.zeros((n_days, n_comids), dtype=np.float32)
    daily_velocity = np.zeros((n_days, n_comids), dtype=np.float32)
    
    print(f"  Computing daily averages for {n_days:,} days × {n_comids:,} comids...")
    
    for i, date in enumerate(unique_dates):
        mask = date_only == date
        
        # Get hourly values for this day
        flow_day = streamflow_hourly[mask, :]
        vel_day = velocity_hourly[mask, :]
        
        # Replace fill values with NaN for averaging
        flow_day = np.where(flow_day == FILL_VALUE, np.nan, flow_day * STREAMFLOW_SCALE)
        vel_day = np.where(vel_day == FILL_VALUE, np.nan, vel_day * VELOCITY_SCALE)
        
        # Compute daily mean (ignoring NaN)
        with np.errstate(all='ignore'):
            daily_flow[i, :] = np.nanmean(flow_day, axis=0)
            daily_velocity[i, :] = np.nanmean(vel_day, axis=0)
        
        if (i + 1) % 365 == 0:
            print(f"    Processed {i + 1:,} days...")
    
    return unique_dates.tolist(), daily_flow, daily_velocity


def insert_flow_history(
    conn,
    comids: np.ndarray,
    dates: list,
    daily_flow: np.ndarray,
    daily_velocity: np.ndarray,
    batch_size: int = 5000  # Smaller batches for memory
):
    """Insert daily flow history into PostgreSQL."""
    print(f"Inserting {len(dates):,} days × {len(comids):,} comids into flow_history...")
    
    total_rows = 0
    batch = []
    
    with conn.cursor() as cur:
        for day_idx, date in enumerate(dates):
            year = date.year
            week = date.isocalendar()[1]
            doy = date.timetuple().tm_yday
            
            for comid_idx, comid in enumerate(comids):
                flow = daily_flow[day_idx, comid_idx]
                vel = daily_velocity[day_idx, comid_idx]
                
                # Skip if both are NaN
                if np.isnan(flow) and np.isnan(vel):
                    continue
                
                # Convert NaN to None for PostgreSQL
                flow = None if np.isnan(flow) else float(flow)
                vel = None if np.isnan(vel) else float(vel)
                
                batch.append((
                    int(comid), date, year, week, doy, flow, vel
                ))
                
                if len(batch) >= batch_size:
                    execute_values(
                        cur,
                        """
                        INSERT INTO flow_history 
                            (comid, date, year, week_of_year, day_of_year, streamflow_cms, velocity_ms)
                        VALUES %s
                        ON CONFLICT (comid, date) DO UPDATE SET
                            streamflow_cms = EXCLUDED.streamflow_cms,
                            velocity_ms = EXCLUDED.velocity_ms
                        """,
                        batch
                    )
                    total_rows += len(batch)
                    batch = []
                    
                    if total_rows % 100000 == 0:
                        print(f"    Inserted {total_rows:,} rows...")
                        conn.commit()
            
            # Progress per day
            if (day_idx + 1) % 100 == 0:
                print(f"  Processed {day_idx + 1:,}/{len(dates):,} days, {total_rows:,} rows inserted")
        
        # Insert remaining batch
        if batch:
            execute_values(
                cur,
                """
                INSERT INTO flow_history 
                    (comid, date, year, week_of_year, day_of_year, streamflow_cms, velocity_ms)
                VALUES %s
                ON CONFLICT (comid, date) DO UPDATE SET
                    streamflow_cms = EXCLUDED.streamflow_cms,
                    velocity_ms = EXCLUDED.velocity_ms
                """,
                batch
            )
            total_rows += len(batch)
    
    conn.commit()
    print(f"  Total rows inserted: {total_rows:,}")
    return total_rows


def fetch_and_load(
    comids: np.ndarray,
    years: int = 10,
    chunk_days: int = 30
):
    """
    Main ETL function. Fetches data in chunks to manage memory.
    """
    print(f"\n{'='*60}")
    print(f"NWM Retrospective ETL")
    print(f"  Comids: {len(comids):,}")
    print(f"  Years: {years}")
    print(f"{'='*60}\n")
    
    # Open Zarr store
    print("Opening Zarr store...")
    zarr_root = open_zarr_store()
    
    # Find comid indices in Zarr
    comid_indices, found_comids = find_comid_indices(zarr_root, comids)
    if len(comid_indices) == 0:
        print("ERROR: No matching comids found in NWM dataset!")
        return
    
    # Get time range
    time_start, time_end, dates = get_time_range(zarr_root, years)
    total_hours = time_end - time_start
    hours_per_chunk = chunk_days * 24
    
    # Connect to database
    print("\nConnecting to PostgreSQL...")
    conn = psycopg2.connect(**DB_CONFIG)
    
    try:
        # Process in time chunks to manage memory
        all_dates = []
        all_flow = []
        all_velocity = []
        
        print(f"\nFetching data in {chunk_days}-day chunks...")
        
        for chunk_start in range(0, total_hours, hours_per_chunk):
            chunk_end = min(chunk_start + hours_per_chunk, total_hours)
            actual_start = time_start + chunk_start
            actual_end = time_start + chunk_end
            
            print(f"\n  Chunk {chunk_start // hours_per_chunk + 1}: hours {actual_start:,}-{actual_end:,}")
            
            # Read streamflow chunk - only for our comids
            # Use Zarr orthogonal indexing to read only needed columns
            # This avoids loading all 2.7M features into memory
            print("    Reading streamflow...")
            sf_subset = zarr_root['streamflow'].get_orthogonal_selection(
                (slice(actual_start, actual_end), comid_indices)
            )
            
            print("    Reading velocity...")
            vel_subset = zarr_root['velocity'].get_orthogonal_selection(
                (slice(actual_start, actual_end), comid_indices)
            )
            
            # Compute daily averages for this chunk
            chunk_dates = dates[chunk_start:chunk_end]
            unique_dates, daily_flow, daily_velocity = compute_daily_averages(
                sf_subset, vel_subset, chunk_dates
            )
            
            del sf_subset, vel_subset
            
            # Insert into database
            insert_flow_history(
                conn, found_comids, unique_dates, daily_flow, daily_velocity
            )
            
            del daily_flow, daily_velocity
        
        print("\n✅ ETL complete!")
        
        # Show summary
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*), MIN(date), MAX(date) FROM flow_history")
            count, min_date, max_date = cur.fetchone()
            print(f"\nflow_history table:")
            print(f"  Rows: {count:,}")
            print(f"  Date range: {min_date} to {max_date}")
        
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description='Fetch NWM retrospective data')
    parser.add_argument('--years', type=int, default=10, help='Years of history to fetch')
    parser.add_argument('--state', type=str, help='State code (e.g., VT)')
    parser.add_argument('--comids', type=str, help='File with comids (one per line)')
    parser.add_argument('--chunk-days', type=int, default=30, help='Days per processing chunk')
    parser.add_argument('--test', action='store_true', help='Test mode: 100 comids, 30 days')
    
    args = parser.parse_args()
    
    # Connect to get comids if needed
    conn = psycopg2.connect(**DB_CONFIG)
    
    try:
        if args.test:
            print("TEST MODE: 100 comids, 30 days")
            comids = get_vermont_comids(conn)[:100]
            years = 0.08  # ~30 days
            chunk_days = 10
        elif args.state and args.state.upper() == 'VT':
            # Vermont is ~14K comids - use smaller chunks to avoid OOM
            comids = get_vermont_comids(conn)
            years = args.years
            chunk_days = min(args.chunk_days, 7)  # Max 7 days for Vermont
            print(f"Vermont mode: using {chunk_days}-day chunks to manage memory")
        elif args.comids:
            comids = load_comids_from_file(args.comids)
            years = args.years
            chunk_days = args.chunk_days
        elif args.state:
            comids = get_state_comids(conn, args.state)
            years = args.years
            chunk_days = args.chunk_days
        else:
            # Default: Vermont
            comids = get_vermont_comids(conn)
            years = args.years
            chunk_days = args.chunk_days
    finally:
        conn.close()
    
    # Run ETL
    fetch_and_load(comids, years=years, chunk_days=chunk_days)


if __name__ == '__main__':
    main()
