#!/usr/bin/env python3
"""
USGS Daily Statistics - Bulk Ingest using dataretrieval

Uses the official USGS dataretrieval Python library for efficient fetching.
Processes by state for optimal batch sizes.
"""

import os
import sys
from datetime import datetime, timezone
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import dataretrieval.nwis as nwis

DATABASE_URL = os.environ.get('DATABASE_URL', 
    'postgresql://river_router:Pacific1ride@river-router-db.c6xmmyu04pdo.us-east-1.rds.amazonaws.com:5432/river_router')

PARAM_CODE = "00060"  # Discharge

# Stats columns we want to save
STAT_COLS = {
    'mean_va': 'mean',
    'min_va': 'min', 
    'max_va': 'max',
    'p05_va': 'p05',
    'p10_va': 'p10',
    'p25_va': 'p25',
    'p50_va': 'p50',
    'p75_va': 'p75',
    'p90_va': 'p90',
    'p95_va': 'p95',
}


def get_db_connection():
    return psycopg2.connect(DATABASE_URL)


def get_sites_by_state():
    """Get sites grouped by state (from HUC codes)."""
    conn = get_db_connection()
    
    # Get sites needing stats, grouped by first 2 digits of site_no (roughly by state)
    query = """
        SELECT g.site_no
        FROM usgs_gauges g
        WHERE g.site_no IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 FROM usgs_statistics s WHERE s.site_no = g.site_no
        )
        ORDER BY g.site_no
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    return df['site_no'].tolist()


def fetch_stats_batch(sites: list) -> pd.DataFrame:
    """Fetch daily statistics for a list of sites using dataretrieval."""
    try:
        result = nwis.get_stats(
            sites=sites,
            parameterCd=PARAM_CODE,
            statReportType="daily"
        )
        
        if isinstance(result, tuple):
            return result[0]
        return result
    except Exception as e:
        print(f"   ⚠️  Error: {e}")
        return pd.DataFrame()


def save_stats(df: pd.DataFrame) -> int:
    """Save statistics dataframe to database."""
    if df.empty:
        return 0
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    records = []
    for _, row in df.iterrows():
        site_no = str(row['site_no'])
        month = int(row['month_nu'])
        day = int(row['day_nu'])
        begin_yr = int(row['begin_yr']) if pd.notna(row.get('begin_yr')) else None
        end_yr = int(row['end_yr']) if pd.notna(row.get('end_yr')) else None
        years = int(row['count_nu']) if pd.notna(row.get('count_nu')) else None
        
        for col, stat_type in STAT_COLS.items():
            if col in row and pd.notna(row[col]):
                records.append((
                    site_no, PARAM_CODE, stat_type, month, day,
                    float(row[col]), years, begin_yr, end_yr,
                    datetime.now(timezone.utc)
                ))
    
    if not records:
        cur.close()
        conn.close()
        return 0
    
    query = """
        INSERT INTO usgs_statistics 
            (site_no, param_code, stat_type, month, day, value, 
             years_of_record, begin_year, end_year, updated_at)
        VALUES %s
        ON CONFLICT (site_no, param_code, stat_type, month, day) 
        DO UPDATE SET
            value = EXCLUDED.value,
            years_of_record = EXCLUDED.years_of_record,
            updated_at = EXCLUDED.updated_at
    """
    
    try:
        execute_values(cur, query, records, page_size=50000)
        conn.commit()
        count = len(records)
    except Exception as e:
        print(f"   DB error: {e}")
        conn.rollback()
        count = 0
    finally:
        cur.close()
        conn.close()
    
    return count


def main(batch_size: int = 100, limit: int = None):
    print("=" * 60)
    print(f"📊 USGS Statistics - Bulk Ingest (dataretrieval)")
    print(f"   Started: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 60)
    
    # Get sites needing stats
    sites = get_sites_by_state()
    if limit:
        sites = sites[:limit]
    
    print(f"\n🔍 {len(sites)} sites need statistics")
    
    if not sites:
        print("✅ All done!")
        return
    
    # Process in batches
    batches = [sites[i:i+batch_size] for i in range(0, len(sites), batch_size)]
    print(f"   {len(batches)} batches of {batch_size} sites")
    
    total_records = 0
    total_sites = 0
    
    for i, batch in enumerate(batches):
        print(f"\n📥 Batch {i+1}/{len(batches)} ({len(batch)} sites)...")
        
        # Fetch from USGS
        df = fetch_stats_batch(batch)
        
        if not df.empty:
            sites_fetched = df['site_no'].nunique()
            
            # Save to database
            count = save_stats(df)
            total_records += count
            total_sites += sites_fetched
            
            print(f"   ✅ {count:,} records for {sites_fetched} sites")
        else:
            print(f"   ⚠️  No data returned")
    
    print(f"\n" + "=" * 60)
    print(f"✅ Complete!")
    print(f"   Sites processed: {total_sites:,}")
    print(f"   Records saved: {total_records:,}")
    print(f"   Finished: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 60)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--batch', type=int, default=100, help='Sites per batch')
    parser.add_argument('--limit', type=int, help='Limit total sites')
    args = parser.parse_args()
    
    main(batch_size=args.batch, limit=args.limit)
