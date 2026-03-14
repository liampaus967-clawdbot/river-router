#!/usr/bin/env python3
"""
NWM Retrospective Data ETL → S3 Parquet
========================================
Fetches historical streamflow/velocity from NOAA's NWM v3.0 Retrospective dataset
and saves daily averages as Parquet files to S3.

Usage:
    python fetch_retrospective_s3.py --years 10 --state VT --bucket my-bucket
    python fetch_retrospective_s3.py --test --bucket my-bucket

Requires .env file with:
    DB_HOST, DB_NAME, DB_USER, DB_PASSWORD (for getting comid list)
    AWS credentials (via env or IAM role)
    S3_BUCKET=your-bucket-name
"""

import argparse
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import psycopg2
import s3fs
import zarr

# Load .env file
env_path = Path(__file__).parent / '.env'
if env_path.exists():
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                os.environ.setdefault(key.strip(), value.strip())

DB_CONFIG = {
    'host': os.environ.get('DB_HOST'),
    'database': os.environ.get('DB_NAME'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'port': int(os.environ.get('DB_PORT', 5432))
}

S3_BUCKET = os.environ.get('S3_BUCKET')
ZARR_PATH = 's3://noaa-nwm-retrospective-3-0-pds/CONUS/zarr/chrtout.zarr'

STREAMFLOW_SCALE = 0.01
VELOCITY_SCALE = 0.01
FILL_VALUE = -999900


def get_vermont_comids(conn) -> np.ndarray:
    """Get Vermont river comids from river_velocities table."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT comid 
            FROM river_velocities 
            WHERE ST_Intersects(geom, ST_MakeEnvelope(-73.5, 42.7, -71.5, 45.1, 4326))
            ORDER BY comid
        """)
        comids = np.array([row[0] for row in cur.fetchall()], dtype=np.int64)
    print(f"Found {len(comids):,} Vermont comids")
    return comids


def open_zarr_store():
    """Open the NWM retrospective Zarr store from S3."""
    s3 = s3fs.S3FileSystem(anon=True)
    store = s3fs.S3Map(root=ZARR_PATH, s3=s3)
    return zarr.open(store, mode='r')


def find_comid_indices(zarr_root, target_comids: np.ndarray):
    """Find indices of target comids in Zarr feature_id array."""
    print("Loading feature_id array from Zarr...")
    feature_ids = zarr_root['feature_id'][:]
    print(f"  Total features in dataset: {len(feature_ids):,}")
    
    feature_id_to_idx = {fid: idx for idx, fid in enumerate(feature_ids)}
    
    indices = []
    found_comids = []
    for comid in target_comids:
        if comid in feature_id_to_idx:
            indices.append(feature_id_to_idx[comid])
            found_comids.append(comid)
    
    print(f"  Matched {len(indices):,} comids")
    return np.array(indices), np.array(found_comids)


def get_time_range(zarr_root, years: int):
    """Get time slice indices for the last N years of data."""
    print("Loading time array from Zarr...")
    time_raw = zarr_root['time'][:]
    
    base_time = datetime(1979, 2, 1, 1, 0, 0)
    last_hours = int(time_raw[-1])
    last_date = base_time + timedelta(hours=last_hours)
    print(f"  Dataset ends: {last_date.strftime('%Y-%m-%d')}")
    
    if years < 1:
        days_back = int(years * 365)
        start_date = last_date - timedelta(days=days_back)
    else:
        start_date = last_date.replace(year=last_date.year - int(years))
    
    start_hours = (start_date - base_time).total_seconds() / 3600
    start_idx = np.searchsorted(time_raw, start_hours)
    end_idx = len(time_raw)
    
    print(f"  Extracting {years} years: {start_date.strftime('%Y-%m-%d')} to {last_date.strftime('%Y-%m-%d')}")
    
    dates = []
    for h in time_raw[start_idx:end_idx]:
        dt = base_time + timedelta(hours=int(h))
        dates.append(dt)
    
    return start_idx, end_idx, np.array(dates)


def compute_daily_averages(streamflow_hourly, velocity_hourly, dates):
    """Compute daily averages from hourly data."""
    date_only = np.array([d.date() for d in dates])
    unique_dates = np.unique(date_only)
    
    n_days = len(unique_dates)
    n_comids = streamflow_hourly.shape[1]
    
    daily_flow = np.zeros((n_days, n_comids), dtype=np.float32)
    daily_velocity = np.zeros((n_days, n_comids), dtype=np.float32)
    
    print(f"  Computing daily averages for {n_days:,} days × {n_comids:,} comids...")
    
    for i, date in enumerate(unique_dates):
        mask = date_only == date
        
        flow_day = streamflow_hourly[mask, :]
        vel_day = velocity_hourly[mask, :]
        
        flow_day = np.where(flow_day == FILL_VALUE, np.nan, flow_day * STREAMFLOW_SCALE)
        vel_day = np.where(vel_day == FILL_VALUE, np.nan, vel_day * VELOCITY_SCALE)
        
        with np.errstate(all='ignore'):
            daily_flow[i, :] = np.nanmean(flow_day, axis=0)
            daily_velocity[i, :] = np.nanmean(vel_day, axis=0)
    
    return unique_dates.tolist(), daily_flow, daily_velocity


def save_to_parquet_s3(bucket: str, state: str, comids: np.ndarray, 
                        dates: list, daily_flow: np.ndarray, daily_velocity: np.ndarray):
    """Save daily averages to Parquet on S3."""
    
    # Flatten to rows
    rows_comid = []
    rows_date = []
    rows_year = []
    rows_week = []
    rows_flow = []
    rows_vel = []
    
    for day_idx, date in enumerate(dates):
        year = date.year
        week = date.isocalendar()[1]
        
        for comid_idx, comid in enumerate(comids):
            flow = daily_flow[day_idx, comid_idx]
            vel = daily_velocity[day_idx, comid_idx]
            
            if np.isnan(flow) and np.isnan(vel):
                continue
            
            rows_comid.append(int(comid))
            rows_date.append(date)
            rows_year.append(year)
            rows_week.append(week)
            rows_flow.append(float(flow) if not np.isnan(flow) else None)
            rows_vel.append(float(vel) if not np.isnan(vel) else None)
    
    print(f"  Prepared {len(rows_comid):,} rows for Parquet")
    
    # Create PyArrow table
    table = pa.table({
        'comid': pa.array(rows_comid, type=pa.int64()),
        'date': pa.array(rows_date, type=pa.date32()),
        'year': pa.array(rows_year, type=pa.int16()),
        'week_of_year': pa.array(rows_week, type=pa.int8()),
        'streamflow_cms': pa.array(rows_flow, type=pa.float32()),
        'velocity_ms': pa.array(rows_vel, type=pa.float32()),
    })
    
    # Write to S3
    s3 = s3fs.S3FileSystem()
    s3_path = f"s3://{bucket}/flow_history/{state.lower()}_flow_history.parquet"
    
    print(f"  Writing to {s3_path}...")
    pq.write_table(table, s3_path, filesystem=s3, compression='snappy')
    
    # Get file size
    info = s3.info(f"{bucket}/flow_history/{state.lower()}_flow_history.parquet")
    size_mb = info['size'] / (1024 * 1024)
    print(f"  ✅ Saved {len(rows_comid):,} rows ({size_mb:.1f} MB)")
    
    return len(rows_comid)


def fetch_and_save(comids: np.ndarray, years: int, bucket: str, state: str, chunk_days: int = 7):
    """Main ETL function."""
    print(f"\n{'='*60}")
    print(f"NWM Retrospective ETL → S3 Parquet")
    print(f"  Comids: {len(comids):,}")
    print(f"  Years: {years}")
    print(f"  Bucket: {bucket}")
    print(f"{'='*60}\n")
    
    zarr_root = open_zarr_store()
    comid_indices, found_comids = find_comid_indices(zarr_root, comids)
    
    if len(comid_indices) == 0:
        print("ERROR: No matching comids!")
        return
    
    time_start, time_end, dates = get_time_range(zarr_root, years)
    total_hours = time_end - time_start
    hours_per_chunk = chunk_days * 24
    
    # Collect all daily data
    all_dates = []
    all_flow = []
    all_velocity = []
    
    print(f"\nFetching data in {chunk_days}-day chunks...")
    
    for chunk_start in range(0, total_hours, hours_per_chunk):
        chunk_end = min(chunk_start + hours_per_chunk, total_hours)
        actual_start = time_start + chunk_start
        actual_end = time_start + chunk_end
        
        chunk_num = chunk_start // hours_per_chunk + 1
        total_chunks = (total_hours + hours_per_chunk - 1) // hours_per_chunk
        print(f"\n  Chunk {chunk_num}/{total_chunks}: hours {actual_start:,}-{actual_end:,}")
        
        print("    Reading streamflow...")
        sf_subset = zarr_root['streamflow'].get_orthogonal_selection(
            (slice(actual_start, actual_end), comid_indices)
        )
        
        print("    Reading velocity...")
        vel_subset = zarr_root['velocity'].get_orthogonal_selection(
            (slice(actual_start, actual_end), comid_indices)
        )
        
        chunk_dates = dates[chunk_start:chunk_end]
        unique_dates, daily_flow, daily_velocity = compute_daily_averages(
            sf_subset, vel_subset, chunk_dates
        )
        
        all_dates.extend(unique_dates)
        all_flow.append(daily_flow)
        all_velocity.append(daily_velocity)
        
        del sf_subset, vel_subset, daily_flow, daily_velocity
    
    # Combine all chunks
    print(f"\nCombining {len(all_dates):,} days of data...")
    combined_flow = np.vstack(all_flow)
    combined_velocity = np.vstack(all_velocity)
    
    # Save to S3
    print(f"\nSaving to S3...")
    total_rows = save_to_parquet_s3(
        bucket, state, found_comids, all_dates, combined_flow, combined_velocity
    )
    
    print(f"\n✅ ETL complete! {total_rows:,} rows saved to S3")


def main():
    parser = argparse.ArgumentParser(description='Fetch NWM retrospective → S3 Parquet')
    parser.add_argument('--years', type=int, default=10)
    parser.add_argument('--state', type=str, default='VT')
    parser.add_argument('--bucket', type=str, default=S3_BUCKET)
    parser.add_argument('--chunk-days', type=int, default=7)
    parser.add_argument('--test', action='store_true', help='Test mode: 100 comids, 30 days')
    args = parser.parse_args()
    
    if not args.bucket:
        print("ERROR: No S3 bucket specified. Use --bucket or set S3_BUCKET in .env")
        return
    
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        if args.test:
            print("TEST MODE: 100 comids, 30 days")
            comids = get_vermont_comids(conn)[:100]
            years = 0.08
            chunk_days = 10
        else:
            comids = get_vermont_comids(conn)
            years = args.years
            chunk_days = args.chunk_days
    finally:
        conn.close()
    
    fetch_and_save(comids, years, args.bucket, args.state, chunk_days)


if __name__ == '__main__':
    main()
