#!/usr/bin/env python3
"""
Compute Flow Percentiles from S3 Parquet
=========================================
Reads flow_history from S3, computes weekly percentiles, 
saves to S3 Parquet AND syncs to PostgreSQL for daily joins.

Usage:
    python compute_percentiles_s3.py --bucket my-bucket --state VT

Requires .env file with:
    DB_HOST, DB_NAME, DB_USER, DB_PASSWORD (for syncing percentiles)
    S3_BUCKET=your-bucket-name
"""

import argparse
import os
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import psycopg2
from psycopg2.extras import execute_values
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

DB_CONFIG = {
    'host': os.environ.get('DB_HOST'),
    'database': os.environ.get('DB_NAME'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'port': int(os.environ.get('DB_PORT', 5432))
}

S3_BUCKET = os.environ.get('S3_BUCKET')


def load_flow_history(bucket: str, state: str) -> pd.DataFrame:
    """Load flow history from S3 Parquet."""
    s3_path = f"s3://{bucket}/flow_history/{state.lower()}_flow_history.parquet"
    print(f"Loading {s3_path}...")
    
    df = pd.read_parquet(s3_path)
    print(f"  Loaded {len(df):,} rows, {df['comid'].nunique():,} comids")
    return df


def compute_percentiles(df: pd.DataFrame) -> pd.DataFrame:
    """Compute weekly percentiles per comid."""
    print("Computing percentiles...")
    
    def calc_stats(group):
        flows = group['streamflow_cms'].dropna()
        vels = group['velocity_ms'].dropna()
        
        if len(flows) < 3:
            return None
        
        return pd.Series({
            'flow_min': flows.min(),
            'flow_p05': np.percentile(flows, 5),
            'flow_p10': np.percentile(flows, 10),
            'flow_p25': np.percentile(flows, 25),
            'flow_p50': np.percentile(flows, 50),
            'flow_p75': np.percentile(flows, 75),
            'flow_p90': np.percentile(flows, 90),
            'flow_p95': np.percentile(flows, 95),
            'flow_max': flows.max(),
            'flow_mean': flows.mean(),
            'flow_stddev': flows.std() if len(flows) > 1 else 0,
            'vel_min': vels.min() if len(vels) >= 3 else None,
            'vel_p05': np.percentile(vels, 5) if len(vels) >= 3 else None,
            'vel_p10': np.percentile(vels, 10) if len(vels) >= 3 else None,
            'vel_p25': np.percentile(vels, 25) if len(vels) >= 3 else None,
            'vel_p50': np.percentile(vels, 50) if len(vels) >= 3 else None,
            'vel_p75': np.percentile(vels, 75) if len(vels) >= 3 else None,
            'vel_p90': np.percentile(vels, 90) if len(vels) >= 3 else None,
            'vel_p95': np.percentile(vels, 95) if len(vels) >= 3 else None,
            'vel_max': vels.max() if len(vels) >= 3 else None,
            'vel_mean': vels.mean() if len(vels) >= 3 else None,
            'vel_stddev': vels.std() if len(vels) > 1 else None,
            'sample_count': len(group),
            'sample_years': group['year'].nunique(),
            'date_range_start': group['date'].min(),
            'date_range_end': group['date'].max(),
        })
    
    result = df.groupby(['comid', 'week_of_year']).apply(calc_stats).dropna()
    result = result.reset_index()
    
    print(f"  Generated {len(result):,} percentile records")
    return result


def save_to_s3(df: pd.DataFrame, bucket: str, state: str):
    """Save percentiles to S3 Parquet."""
    s3_path = f"s3://{bucket}/flow_percentiles/{state.lower()}_percentiles.parquet"
    print(f"Saving to {s3_path}...")
    
    df.to_parquet(s3_path, index=False, compression='snappy')
    
    s3 = s3fs.S3FileSystem()
    info = s3.info(f"{bucket}/flow_percentiles/{state.lower()}_percentiles.parquet")
    size_kb = info['size'] / 1024
    print(f"  ✅ Saved ({size_kb:.1f} KB)")


def sync_to_postgres(df: pd.DataFrame):
    """Sync percentiles to PostgreSQL for fast daily joins."""
    print("Syncing to PostgreSQL...")
    
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        # Convert DataFrame to list of tuples
        rows = []
        for _, row in df.iterrows():
            rows.append((
                int(row['comid']), int(row['week_of_year']),
                row['flow_min'], row['flow_p05'], row['flow_p10'], row['flow_p25'],
                row['flow_p50'], row['flow_p75'], row['flow_p90'], row['flow_p95'],
                row['flow_max'], row['flow_mean'], row['flow_stddev'],
                row['vel_min'], row['vel_p05'], row['vel_p10'], row['vel_p25'],
                row['vel_p50'], row['vel_p75'], row['vel_p90'], row['vel_p95'],
                row['vel_max'], row['vel_mean'], row['vel_stddev'],
                int(row['sample_count']), int(row['sample_years']),
                row['date_range_start'], row['date_range_end']
            ))
        
        with conn.cursor() as cur:
            # Clear existing data for these comids
            comids = df['comid'].unique().tolist()
            cur.execute("DELETE FROM flow_percentiles WHERE comid = ANY(%s)", (comids,))
            
            # Insert new data
            execute_values(
                cur,
                """
                INSERT INTO flow_percentiles (
                    comid, week_of_year,
                    flow_min, flow_p05, flow_p10, flow_p25, flow_p50, flow_p75, flow_p90, flow_p95,
                    flow_max, flow_mean, flow_stddev,
                    vel_min, vel_p05, vel_p10, vel_p25, vel_p50, vel_p75, vel_p90, vel_p95,
                    vel_max, vel_mean, vel_stddev,
                    sample_count, sample_years, date_range_start, date_range_end
                ) VALUES %s
                """,
                rows
            )
        
        conn.commit()
        print(f"  ✅ Synced {len(rows):,} rows to PostgreSQL")
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description='Compute percentiles from S3')
    parser.add_argument('--bucket', type=str, default=S3_BUCKET)
    parser.add_argument('--state', type=str, default='VT')
    parser.add_argument('--skip-postgres', action='store_true', help='Skip PostgreSQL sync')
    args = parser.parse_args()
    
    if not args.bucket:
        print("ERROR: No S3 bucket specified. Use --bucket or set S3_BUCKET in .env")
        return
    
    # Load from S3
    df = load_flow_history(args.bucket, args.state)
    
    # Compute percentiles
    percentiles_df = compute_percentiles(df)
    
    # Save to S3
    save_to_s3(percentiles_df, args.bucket, args.state)
    
    # Sync to PostgreSQL
    if not args.skip_postgres:
        sync_to_postgres(percentiles_df)
    
    print(f"\n✅ Done!")
    
    # Summary
    print(f"\nSummary:")
    print(f"  Comids: {percentiles_df['comid'].nunique():,}")
    print(f"  Weeks: {percentiles_df['week_of_year'].nunique()}")
    print(f"  Total records: {len(percentiles_df):,}")


if __name__ == '__main__':
    main()
