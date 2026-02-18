#!/usr/bin/env python3
"""
USGS Daily Statistics - Parallel Ingest

Uses asyncio/aiohttp to fetch multiple batches simultaneously.
10-20x faster than sequential approach.
"""

import os
import sys
import asyncio
import aiohttp
from datetime import datetime, timezone
from typing import List, Dict, Optional
import psycopg2
from psycopg2.extras import execute_values

DATABASE_URL = os.environ.get('DATABASE_URL', 
    'postgresql://river_router:Pacific1ride@river-router-db.c6xmmyu04pdo.us-east-1.rds.amazonaws.com:5432/river_router')

USGS_STAT_BASE = "https://waterservices.usgs.gov/nwis/stat/"
PARAM_CODE = "00060"
SITES_PER_BATCH = 10
CONCURRENT_REQUESTS = 5  # Be nice to USGS, but still fast
DELAY_BETWEEN_BATCHES = 0.2  # Seconds


def get_db_connection():
    return psycopg2.connect(DATABASE_URL)


def get_gauges_needing_stats() -> List[str]:
    """Get site numbers that need statistics."""
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT DISTINCT g.site_no
        FROM usgs_gauges g
        WHERE g.site_no IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 FROM usgs_statistics s 
            WHERE s.site_no = g.site_no
        )
        ORDER BY g.site_no
    """)
    
    sites = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return sites


def parse_rdb_response(text: str) -> List[Dict]:
    """Parse RDB format response into records."""
    records = []
    lines = text.strip().split('\n')
    
    # Find header
    header_idx = None
    for i, line in enumerate(lines):
        if not line.startswith('#'):
            header_idx = i
            break
    
    if header_idx is None:
        return []
    
    headers = lines[header_idx].split('\t')
    data_start = header_idx + 2
    
    # Map columns
    col_map = {h.lower().strip(): i for i, h in enumerate(headers)}
    
    stat_columns = {
        'mean': 'mean_va', 'p05': 'p05_va', 'p10': 'p10_va',
        'p25': 'p25_va', 'p50': 'p50_va', 'p75': 'p75_va',
        'p90': 'p90_va', 'p95': 'p95_va', 'min': 'min_va', 'max': 'max_va',
    }
    
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
            
            begin_year = int(cols[col_map['begin_yr']]) if col_map.get('begin_yr') and cols[col_map['begin_yr']].strip() else None
            end_year = int(cols[col_map['end_yr']]) if col_map.get('end_yr') and cols[col_map['end_yr']].strip() else None
            years = int(cols[col_map['count_nu']]) if col_map.get('count_nu') and cols[col_map['count_nu']].strip() else None
            
            for stat_name, col_name in stat_columns.items():
                if col_name in col_map:
                    val_str = cols[col_map[col_name]].strip()
                    if val_str:
                        try:
                            records.append({
                                'site_no': site_no,
                                'param_code': PARAM_CODE,
                                'stat_type': stat_name,
                                'month': month,
                                'day': day,
                                'value': float(val_str),
                                'years_of_record': years,
                                'begin_year': begin_year,
                                'end_year': end_year,
                            })
                        except ValueError:
                            continue
        except (ValueError, IndexError, KeyError):
            continue
    
    return records


async def fetch_batch(session: aiohttp.ClientSession, sites: List[str], semaphore: asyncio.Semaphore) -> List[Dict]:
    """Fetch statistics for a batch of sites."""
    async with semaphore:
        params = {
            'format': 'rdb',
            'sites': ','.join(sites),
            'parameterCd': PARAM_CODE,
            'statReportType': 'daily',
        }
        
        try:
            async with session.get(USGS_STAT_BASE, params=params, timeout=aiohttp.ClientTimeout(total=120)) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    return parse_rdb_response(text)
                else:
                    return []
        except Exception as e:
            return []


def save_records(records: List[Dict]) -> int:
    """Save records to database."""
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
            updated_at = EXCLUDED.updated_at
    """
    
    try:
        execute_values(cur, query, values, page_size=10000)
        conn.commit()
        count = len(values)
    except Exception as e:
        print(f"DB error: {e}")
        conn.rollback()
        count = 0
    finally:
        cur.close()
        conn.close()
    
    return count


async def main():
    print("=" * 60)
    print(f"📊 USGS Statistics - Parallel Ingest")
    print(f"   Started: {datetime.now(timezone.utc).isoformat()}")
    print(f"   Concurrent requests: {CONCURRENT_REQUESTS}")
    print("=" * 60)
    
    # Get sites needing stats
    sites = get_gauges_needing_stats()
    print(f"\n🔍 {len(sites)} gauges need statistics")
    
    if not sites:
        print("✅ All done!")
        return
    
    # Create batches
    batches = [sites[i:i+SITES_PER_BATCH] for i in range(0, len(sites), SITES_PER_BATCH)]
    print(f"   {len(batches)} batches of {SITES_PER_BATCH} sites")
    
    # Semaphore for rate limiting
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
    
    total_records = 0
    total_sites = 0
    
    async with aiohttp.ClientSession() as session:
        # Process in chunks to show progress
        chunk_size = 50  # Process 50 batches, then save & report
        
        for chunk_start in range(0, len(batches), chunk_size):
            chunk_batches = batches[chunk_start:chunk_start + chunk_size]
            chunk_num = chunk_start // chunk_size + 1
            total_chunks = (len(batches) + chunk_size - 1) // chunk_size
            
            print(f"\n📥 Chunk {chunk_num}/{total_chunks} ({len(chunk_batches)} batches)...")
            
            # Fetch all batches in this chunk concurrently
            tasks = [fetch_batch(session, batch, semaphore) for batch in chunk_batches]
            results = await asyncio.gather(*tasks)
            
            # Combine all records
            all_records = []
            sites_in_chunk = set()
            for records in results:
                all_records.extend(records)
                for r in records:
                    sites_in_chunk.add(r['site_no'])
            
            # Save to database
            if all_records:
                count = save_records(all_records)
                total_records += count
                total_sites += len(sites_in_chunk)
                print(f"   ✅ {count:,} records for {len(sites_in_chunk)} sites")
            
            # Small delay between chunks
            await asyncio.sleep(DELAY_BETWEEN_BATCHES * chunk_size)
    
    print(f"\n" + "=" * 60)
    print(f"✅ Complete!")
    print(f"   Sites: {total_sites:,}")
    print(f"   Records: {total_records:,}")
    print(f"   Finished: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
