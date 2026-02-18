#!/usr/bin/env python3
"""
USGS Daily Statistics Ingest Script

Fetches historical daily flow statistics from the USGS Statistics API.
This data enables:
- Flow condition badges (Low/Normal/High)
- "X% faster/slower than average" messaging
- Percentile-based runability thresholds

Data source: https://waterservices.usgs.gov/docs/statistics/
Endpoint: https://waterservices.usgs.gov/nwis/stat/

Statistics available:
- mean: Daily mean value
- p05, p10, p25, p50, p75, p90, p95: Percentiles
- min, max: Historical extremes

Rate limits: ~100 sites per request, be respectful of USGS servers
"""

import os
import sys
import time
import requests
from datetime import datetime, timezone
from typing import List, Dict, Optional
import psycopg2
from psycopg2.extras import execute_values

# Database connection
DATABASE_URL = os.environ.get('DATABASE_URL', 
    'postgresql://river_router:Pacific1ride@river-router-db.c6xmmyu04pdo.us-east-1.rds.amazonaws.com:5432/river_router')

# USGS API settings
USGS_STAT_BASE = "https://waterservices.usgs.gov/nwis/stat/"
PARAM_CODE = "00060"  # Discharge (cfs)
STAT_TYPES = ["mean", "p10", "p25", "p50", "p75", "p90"]  # Percentiles we want
SITES_PER_REQUEST = 10  # Smaller batches to avoid URL length issues
REQUEST_DELAY = 1.0  # Seconds between requests (be nice to USGS)


def get_db_connection():
    """Get PostgreSQL connection."""
    return psycopg2.connect(DATABASE_URL)


def get_gauges_to_process(limit: Optional[int] = None) -> List[Dict]:
    """
    Get USGS gauges that need statistics populated.
    Returns gauges that have readings but no statistics yet.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    
    query = """
        SELECT DISTINCT g.site_no, g.site_name, g.state_cd
        FROM usgs_gauges g
        WHERE g.site_no IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 FROM usgs_statistics s 
            WHERE s.site_no = g.site_no
        )
        ORDER BY g.state_cd, g.site_no
    """
    
    if limit:
        query += f" LIMIT {limit}"
    
    cur.execute(query)
    gauges = [{'site_no': row[0], 'site_name': row[1], 'state_cd': row[2]} 
              for row in cur.fetchall()]
    
    cur.close()
    conn.close()
    
    return gauges


def get_all_gauges() -> List[Dict]:
    """Get all USGS gauges from database."""
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT site_no, site_name, state_cd 
        FROM usgs_gauges 
        WHERE site_no IS NOT NULL
        ORDER BY state_cd, site_no
    """)
    
    gauges = [{'site_no': row[0], 'site_name': row[1], 'state_cd': row[2]} 
              for row in cur.fetchall()]
    
    cur.close()
    conn.close()
    
    return gauges


def fetch_statistics_batch(site_nos: List[str]) -> List[Dict]:
    """
    Fetch daily statistics for a batch of sites from USGS API.
    
    Returns list of stat records ready for database insertion.
    """
    if not site_nos:
        return []
    
    sites_str = ','.join(site_nos)
    
    params = {
        'format': 'json',
        'sites': sites_str,
        'parameterCd': PARAM_CODE,
        'statReportType': 'daily',
        'statTypeCd': ','.join(STAT_TYPES),
    }
    
    try:
        response = requests.get(USGS_STAT_BASE, params=params, timeout=120)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        print(f"   ⚠️  API request failed: {e}")
        return []
    except ValueError as e:
        print(f"   ⚠️  JSON parse failed: {e}")
        return []
    
    # Parse response
    records = []
    
    if 'value' not in data or 'timeSeries' not in data['value']:
        return []
    
    for ts in data['value']['timeSeries']:
        try:
            site_no = ts['sourceInfo']['siteCode'][0]['value']
            
            # Get parameter code
            variable = ts['variable']
            param_code = variable['variableCode'][0]['value']
            
            if param_code != PARAM_CODE:
                continue
            
            # Extract statistics
            for stat_entry in ts.get('values', []):
                stat_type = stat_entry.get('method', [{}])[0].get('methodID', '')
                
                # Parse stat type from method description or use default
                stat_info = stat_entry.get('qualifier', [])
                
                for value_entry in stat_entry.get('value', []):
                    try:
                        # Get the statistic type
                        stat_cd = value_entry.get('qualifiers', [''])[0] if value_entry.get('qualifiers') else 'mean'
                        
                        # Parse month/day from the value
                        # USGS returns data keyed by month-day
                        date_str = value_entry.get('dateTime', '')
                        if date_str:
                            # Format: "2024-01-15T00:00:00.000"
                            dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                            month = dt.month
                            day = dt.day
                        else:
                            continue
                        
                        val = value_entry.get('value')
                        if val is None or val == '':
                            continue
                        
                        val = float(val)
                        
                        # Get years of record info if available
                        years = value_entry.get('qualifiers', [])
                        
                        records.append({
                            'site_no': site_no,
                            'param_code': param_code,
                            'stat_type': stat_cd,
                            'month': month,
                            'day': day,
                            'value': val,
                            'years_of_record': None,  # Could parse from response
                            'begin_year': None,
                            'end_year': None,
                        })
                        
                    except (ValueError, KeyError, IndexError) as e:
                        continue
                        
        except (KeyError, IndexError) as e:
            continue
    
    return records


def fetch_statistics_rdb(site_nos: List[str]) -> List[Dict]:
    """
    Fetch daily statistics using RDB format (more reliable parsing).
    """
    if not site_nos:
        return []
    
    sites_str = ','.join(site_nos)
    
    params = {
        'format': 'rdb',
        'sites': sites_str,
        'parameterCd': PARAM_CODE,
        'statReportType': 'daily',
        # Don't specify statTypeCd - API returns all stats by default
    }
    
    try:
        response = requests.get(USGS_STAT_BASE, params=params, timeout=120)
        response.raise_for_status()
        text = response.text
    except requests.exceptions.RequestException as e:
        print(f"   ⚠️  API request failed: {e}")
        return []
    
    # Parse RDB format
    records = []
    lines = text.strip().split('\n')
    
    # Find header line (first line not starting with #)
    header_idx = None
    for i, line in enumerate(lines):
        if not line.startswith('#'):
            header_idx = i
            break
    
    if header_idx is None or header_idx >= len(lines):
        return []
    
    headers = lines[header_idx].split('\t')
    
    # Data starts after format line (5s, 15s, etc.)
    data_start = header_idx + 2
    
    # Map header names to column indices
    col_map = {}
    for i, h in enumerate(headers):
        h_lower = h.lower().strip()
        col_map[h_lower] = i
    
    # Parse data rows
    for line in lines[data_start:]:
        if not line or line.startswith('#'):
            continue
        
        cols = line.split('\t')
        if len(cols) < 10:
            continue
        
        try:
            site_no = cols[col_map.get('site_no', 1)].strip()
            month = int(cols[col_map.get('month_nu', 5)]) if cols[col_map.get('month_nu', 5)].strip() else None
            day = int(cols[col_map.get('day_nu', 6)]) if cols[col_map.get('day_nu', 6)].strip() else None
            
            if not site_no or not month or not day:
                continue
            
            # Get metadata
            begin_year = int(cols[col_map['begin_yr']]) if col_map.get('begin_yr') and cols[col_map['begin_yr']].strip() else None
            end_year = int(cols[col_map['end_yr']]) if col_map.get('end_yr') and cols[col_map['end_yr']].strip() else None
            years_of_record = int(cols[col_map['count_nu']]) if col_map.get('count_nu') and cols[col_map['count_nu']].strip() else None
            
            # Extract each stat type present in the data
            stat_columns = {
                'mean': 'mean_va',
                'p05': 'p05_va',
                'p10': 'p10_va',
                'p25': 'p25_va',
                'p50': 'p50_va',
                'p75': 'p75_va',
                'p90': 'p90_va',
                'p95': 'p95_va',
                'min': 'min_va',
                'max': 'max_va',
            }
            
            for stat_name, col_name in stat_columns.items():
                if col_name in col_map:
                    val_str = cols[col_map[col_name]].strip()
                    if val_str and val_str != '':
                        try:
                            val = float(val_str)
                            records.append({
                                'site_no': site_no,
                                'param_code': PARAM_CODE,
                                'stat_type': stat_name,
                                'month': month,
                                'day': day,
                                'value': val,
                                'years_of_record': years_of_record,
                                'begin_year': begin_year,
                                'end_year': end_year,
                            })
                        except ValueError:
                            continue
                            
        except (ValueError, IndexError, KeyError) as e:
            continue
    
    return records


def save_statistics(records: List[Dict]) -> int:
    """Save statistics records to database."""
    if not records:
        return 0
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    values = [
        (r['site_no'], r['param_code'], r['stat_type'], r['month'], r['day'],
         r['value'], r['years_of_record'], r['begin_year'], r['end_year'],
         datetime.now(timezone.utc))
        for r in records
    ]
    
    query = """
        INSERT INTO usgs_statistics 
            (site_no, param_code, stat_type, month, day, value, 
             years_of_record, begin_year, end_year, updated_at)
        VALUES %s
        ON CONFLICT (site_no, param_code, stat_type, month, day) 
        DO UPDATE SET
            value = EXCLUDED.value,
            years_of_record = EXCLUDED.years_of_record,
            begin_year = EXCLUDED.begin_year,
            end_year = EXCLUDED.end_year,
            updated_at = EXCLUDED.updated_at
    """
    
    try:
        execute_values(cur, query, values, page_size=10000)
        conn.commit()
        count = len(values)
    except Exception as e:
        print(f"   ⚠️  Database error: {e}")
        conn.rollback()
        count = 0
    finally:
        cur.close()
        conn.close()
    
    return count


def ensure_table_exists():
    """Ensure usgs_statistics table has proper structure."""
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Add unique constraint if not exists
    cur.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint 
                WHERE conname = 'usgs_statistics_unique_key'
            ) THEN
                ALTER TABLE usgs_statistics 
                ADD CONSTRAINT usgs_statistics_unique_key 
                UNIQUE (site_no, param_code, stat_type, month, day);
            END IF;
        END $$;
    """)
    
    conn.commit()
    cur.close()
    conn.close()


def main(limit: Optional[int] = None, refresh_all: bool = False):
    """
    Main ingest function.
    
    Args:
        limit: Max number of sites to process (None = all)
        refresh_all: If True, process all gauges even if they have stats
    """
    print("=" * 60)
    print(f"📊 USGS Daily Statistics Ingest")
    print(f"   Started: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 60)
    
    # Ensure table structure
    ensure_table_exists()
    
    # Get gauges to process
    if refresh_all:
        gauges = get_all_gauges()
        if limit:
            gauges = gauges[:limit]
        print(f"\n🔄 Refresh mode: processing all {len(gauges)} gauges")
    else:
        gauges = get_gauges_to_process(limit)
        print(f"\n🔍 Found {len(gauges)} gauges needing statistics")
    
    if not gauges:
        print("✅ All gauges already have statistics!")
        return
    
    # Process in batches
    total_records = 0
    total_sites = 0
    failed_batches = 0
    
    site_nos = [g['site_no'] for g in gauges]
    batches = [site_nos[i:i+SITES_PER_REQUEST] for i in range(0, len(site_nos), SITES_PER_REQUEST)]
    
    print(f"   Processing {len(batches)} batches of {SITES_PER_REQUEST} sites each")
    print()
    
    for batch_idx, batch in enumerate(batches):
        print(f"📥 Batch {batch_idx + 1}/{len(batches)} ({len(batch)} sites)...")
        
        # Fetch statistics (try RDB format first, more reliable)
        records = fetch_statistics_rdb(batch)
        
        if not records:
            # Fallback to JSON
            records = fetch_statistics_batch(batch)
        
        if records:
            # Save to database
            count = save_statistics(records)
            total_records += count
            sites_in_batch = len(set(r['site_no'] for r in records))
            total_sites += sites_in_batch
            print(f"   ✅ Saved {count:,} records for {sites_in_batch} sites")
        else:
            failed_batches += 1
            print(f"   ⚠️  No data returned for batch")
        
        # Rate limiting
        if batch_idx < len(batches) - 1:
            time.sleep(REQUEST_DELAY)
    
    print()
    print("=" * 60)
    print(f"✅ Complete!")
    print(f"   Sites processed: {total_sites:,}")
    print(f"   Records saved: {total_records:,}")
    print(f"   Failed batches: {failed_batches}")
    print(f"   Finished: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 60)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Ingest USGS daily statistics')
    parser.add_argument('--limit', type=int, help='Limit number of sites to process')
    parser.add_argument('--refresh', action='store_true', help='Refresh all gauges, not just new ones')
    
    args = parser.parse_args()
    
    main(limit=args.limit, refresh_all=args.refresh)
