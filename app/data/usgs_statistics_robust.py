#!/usr/bin/env python3
"""
USGS Statistics - Robust single-site fetcher with error handling.
Processes sites one at a time to handle missing/error cases gracefully.
"""

import os
import sys
import time
from datetime import datetime, timezone
import psycopg2
from psycopg2.extras import execute_values
import dataretrieval.nwis as nwis

DATABASE_URL = os.environ.get('DATABASE_URL', 
    'postgresql://river_router:Pacific1ride@river-router-db.c6xmmyu04pdo.us-east-1.rds.amazonaws.com:5432/river_router')

STAT_COLS = {'mean_va': 'mean', 'p10_va': 'p10', 'p25_va': 'p25', 
             'p50_va': 'p50', 'p75_va': 'p75', 'p90_va': 'p90',
             'min_va': 'min', 'max_va': 'max'}


def get_sites_needing_stats(limit=None):
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    
    query = """
        SELECT DISTINCT g.site_no 
        FROM usgs_gauges g
        WHERE g.site_no IS NOT NULL
        AND NOT EXISTS (SELECT 1 FROM usgs_statistics s WHERE s.site_no = g.site_no)
        ORDER BY g.site_no
    """
    if limit:
        query += f" LIMIT {limit}"
    
    cur.execute(query)
    sites = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return sites


def fetch_and_save_site(site_no):
    """Fetch stats for one site and save to DB. Returns record count or -1 on error."""
    try:
        result = nwis.get_stats(sites=site_no, parameterCd="00060", statReportType="daily")
        
        if not isinstance(result, tuple) or result[0].empty:
            return 0  # No data available
        
        df = result[0]
        
        # Build records
        records = []
        for _, row in df.iterrows():
            for col, stat in STAT_COLS.items():
                if col in row.index and row[col] is not None:
                    try:
                        records.append((
                            site_no, "00060", stat, 
                            int(row['month_nu']), int(row['day_nu']),
                            float(row[col]),
                            int(row['count_nu']) if 'count_nu' in row and row['count_nu'] else None,
                            int(row['begin_yr']) if 'begin_yr' in row and row['begin_yr'] else None,
                            int(row['end_yr']) if 'end_yr' in row and row['end_yr'] else None,
                            datetime.now(timezone.utc)
                        ))
                    except:
                        pass
        
        if not records:
            return 0
        
        # Save to DB
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        execute_values(cur, """
            INSERT INTO usgs_statistics 
            (site_no, param_code, stat_type, month, day, value, 
             years_of_record, begin_year, end_year, updated_at)
            VALUES %s 
            ON CONFLICT (site_no, param_code, stat_type, month, day) DO NOTHING
        """, records)
        
        conn.commit()
        cur.close()
        conn.close()
        
        return len(records)
        
    except Exception as e:
        return -1  # Error


def main(limit=None):
    print("=" * 60)
    print(f"📊 USGS Statistics - Robust Ingest")
    print(f"   {datetime.now(timezone.utc).isoformat()}")
    print("=" * 60)
    
    sites = get_sites_needing_stats(limit)
    print(f"\n🔍 {len(sites)} sites to process")
    
    total_records = 0
    success = 0
    no_data = 0
    errors = 0
    
    for i, site in enumerate(sites):
        result = fetch_and_save_site(site)
        
        if result > 0:
            total_records += result
            success += 1
            status = f"✓ {result:,} records"
        elif result == 0:
            no_data += 1
            status = "- no data"
        else:
            errors += 1
            status = "✗ error"
        
        # Progress every 50 sites
        if (i + 1) % 50 == 0:
            print(f"   [{i+1}/{len(sites)}] {success} ok, {no_data} empty, {errors} err | {total_records:,} records")
        
        # Small delay to be nice to USGS
        time.sleep(0.1)
    
    print(f"\n" + "=" * 60)
    print(f"✅ Complete!")
    print(f"   Success: {success} sites ({total_records:,} records)")
    print(f"   No data: {no_data} sites")
    print(f"   Errors:  {errors} sites")
    print("=" * 60)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--limit', type=int, help='Limit sites to process')
    args = parser.parse_args()
    main(args.limit)
