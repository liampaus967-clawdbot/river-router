#!/usr/bin/env python3
"""
Compute Flow Percentiles
========================
Aggregates flow_history into weekly percentile breakpoints per comid.

Usage:
    python compute_percentiles.py              # All comids in flow_history
    python compute_percentiles.py --comid 123  # Single comid (for testing)
    python compute_percentiles.py --dry-run    # Show stats without inserting

Requires .env file with:
    DB_HOST=...
    DB_NAME=...
    DB_USER=...
    DB_PASSWORD=...
    DB_PORT=5432
"""

import argparse
import os
from datetime import datetime
from pathlib import Path

import numpy as np
import psycopg2
from psycopg2.extras import execute_values

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


def compute_percentiles_for_comid(cur, comid: int) -> list[tuple]:
    """
    Compute weekly percentiles for a single comid.
    Returns list of (comid, week, flow_stats..., vel_stats..., metadata...) tuples.
    """
    # Get all flow history for this comid, grouped by week
    cur.execute("""
        SELECT 
            week_of_year,
            array_agg(streamflow_cms) FILTER (WHERE streamflow_cms IS NOT NULL) as flows,
            array_agg(velocity_ms) FILTER (WHERE velocity_ms IS NOT NULL) as velocities,
            COUNT(*) as sample_count,
            COUNT(DISTINCT year) as sample_years,
            MIN(date) as date_start,
            MAX(date) as date_end
        FROM flow_history
        WHERE comid = %s
        GROUP BY week_of_year
        ORDER BY week_of_year
    """, (comid,))
    
    results = []
    for row in cur.fetchall():
        week = row[0]
        flows = np.array(row[1]) if row[1] else np.array([])
        velocities = np.array(row[2]) if row[2] else np.array([])
        sample_count = row[3]
        sample_years = row[4]
        date_start = row[5]
        date_end = row[6]
        
        # Need at least a few samples for meaningful percentiles
        if len(flows) < 3:
            continue
        
        # Compute flow percentiles
        flow_stats = {
            'min': float(np.min(flows)),
            'p05': float(np.percentile(flows, 5)),
            'p10': float(np.percentile(flows, 10)),
            'p25': float(np.percentile(flows, 25)),
            'p50': float(np.percentile(flows, 50)),
            'p75': float(np.percentile(flows, 75)),
            'p90': float(np.percentile(flows, 90)),
            'p95': float(np.percentile(flows, 95)),
            'max': float(np.max(flows)),
            'mean': float(np.mean(flows)),
            'stddev': float(np.std(flows)) if len(flows) > 1 else 0.0
        }
        
        # Compute velocity percentiles
        if len(velocities) >= 3:
            vel_stats = {
                'min': float(np.min(velocities)),
                'p05': float(np.percentile(velocities, 5)),
                'p10': float(np.percentile(velocities, 10)),
                'p25': float(np.percentile(velocities, 25)),
                'p50': float(np.percentile(velocities, 50)),
                'p75': float(np.percentile(velocities, 75)),
                'p90': float(np.percentile(velocities, 90)),
                'p95': float(np.percentile(velocities, 95)),
                'max': float(np.max(velocities)),
                'mean': float(np.mean(velocities)),
                'stddev': float(np.std(velocities)) if len(velocities) > 1 else 0.0
            }
        else:
            vel_stats = {k: None for k in ['min', 'p05', 'p10', 'p25', 'p50', 'p75', 'p90', 'p95', 'max', 'mean', 'stddev']}
        
        results.append((
            comid, week,
            # Flow stats
            flow_stats['min'], flow_stats['p05'], flow_stats['p10'], flow_stats['p25'],
            flow_stats['p50'], flow_stats['p75'], flow_stats['p90'], flow_stats['p95'],
            flow_stats['max'], flow_stats['mean'], flow_stats['stddev'],
            # Velocity stats
            vel_stats['min'], vel_stats['p05'], vel_stats['p10'], vel_stats['p25'],
            vel_stats['p50'], vel_stats['p75'], vel_stats['p90'], vel_stats['p95'],
            vel_stats['max'], vel_stats['mean'], vel_stats['stddev'],
            # Metadata
            sample_count, sample_years, date_start, date_end
        ))
    
    return results


def compute_all_percentiles(conn, dry_run: bool = False, single_comid: int = None):
    """Compute percentiles for all comids in flow_history."""
    
    with conn.cursor() as cur:
        # Get list of comids to process
        if single_comid:
            comids = [single_comid]
        else:
            cur.execute("SELECT DISTINCT comid FROM flow_history ORDER BY comid")
            comids = [row[0] for row in cur.fetchall()]
        
        print(f"Computing percentiles for {len(comids):,} comids...")
        
        all_results = []
        for i, comid in enumerate(comids):
            results = compute_percentiles_for_comid(cur, comid)
            all_results.extend(results)
            
            if (i + 1) % 1000 == 0:
                print(f"  Processed {i + 1:,}/{len(comids):,} comids...")
        
        print(f"\nGenerated {len(all_results):,} percentile records")
        
        if dry_run:
            print("\n[DRY RUN] Would insert these records. Sample:")
            for r in all_results[:3]:
                print(f"  comid={r[0]}, week={r[1]}, flow_p50={r[6]:.3f}, vel_p50={r[17]:.3f if r[17] else 'N/A'}")
            return
        
        # Insert into flow_percentiles
        print("\nInserting into flow_percentiles...")
        execute_values(
            cur,
            """
            INSERT INTO flow_percentiles (
                comid, week_of_year,
                flow_min, flow_p05, flow_p10, flow_p25, flow_p50, flow_p75, flow_p90, flow_p95, flow_max, flow_mean, flow_stddev,
                vel_min, vel_p05, vel_p10, vel_p25, vel_p50, vel_p75, vel_p90, vel_p95, vel_max, vel_mean, vel_stddev,
                sample_count, sample_years, date_range_start, date_range_end
            ) VALUES %s
            ON CONFLICT (comid, week_of_year) DO UPDATE SET
                flow_min = EXCLUDED.flow_min,
                flow_p05 = EXCLUDED.flow_p05,
                flow_p10 = EXCLUDED.flow_p10,
                flow_p25 = EXCLUDED.flow_p25,
                flow_p50 = EXCLUDED.flow_p50,
                flow_p75 = EXCLUDED.flow_p75,
                flow_p90 = EXCLUDED.flow_p90,
                flow_p95 = EXCLUDED.flow_p95,
                flow_max = EXCLUDED.flow_max,
                flow_mean = EXCLUDED.flow_mean,
                flow_stddev = EXCLUDED.flow_stddev,
                vel_min = EXCLUDED.vel_min,
                vel_p05 = EXCLUDED.vel_p05,
                vel_p10 = EXCLUDED.vel_p10,
                vel_p25 = EXCLUDED.vel_p25,
                vel_p50 = EXCLUDED.vel_p50,
                vel_p75 = EXCLUDED.vel_p75,
                vel_p90 = EXCLUDED.vel_p90,
                vel_p95 = EXCLUDED.vel_p95,
                vel_max = EXCLUDED.vel_max,
                vel_mean = EXCLUDED.vel_mean,
                vel_stddev = EXCLUDED.vel_stddev,
                sample_count = EXCLUDED.sample_count,
                sample_years = EXCLUDED.sample_years,
                date_range_start = EXCLUDED.date_range_start,
                date_range_end = EXCLUDED.date_range_end,
                computed_at = NOW()
            """,
            all_results
        )
        conn.commit()
        
        print(f"✅ Inserted {len(all_results):,} records into flow_percentiles")


def show_summary(conn):
    """Show summary of computed percentiles."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT comid) as unique_comids,
                COUNT(DISTINCT week_of_year) as unique_weeks,
                AVG(sample_count) as avg_samples,
                AVG(sample_years) as avg_years
            FROM flow_percentiles
        """)
        row = cur.fetchone()
        print(f"\n📊 flow_percentiles summary:")
        print(f"   Records: {row[0]:,}")
        print(f"   Comids: {row[1]:,}")
        print(f"   Weeks: {row[2]}")
        print(f"   Avg samples/week: {row[3]:.1f}")
        print(f"   Avg years of data: {row[4]:.1f}")
        
        # Show sample percentiles
        cur.execute("""
            SELECT comid, week_of_year, flow_p50, flow_p90, vel_p50
            FROM flow_percentiles
            ORDER BY comid, week_of_year
            LIMIT 5
        """)
        print(f"\n   Sample records:")
        for row in cur.fetchall():
            print(f"   comid={row[0]}, week={row[1]}: flow_p50={row[2]:.3f} m³/s, flow_p90={row[3]:.3f} m³/s, vel_p50={row[4]:.2f} m/s")


def main():
    parser = argparse.ArgumentParser(description='Compute flow percentiles')
    parser.add_argument('--comid', type=int, help='Process single comid')
    parser.add_argument('--dry-run', action='store_true', help='Show stats without inserting')
    args = parser.parse_args()
    
    print("Connecting to PostgreSQL...")
    conn = psycopg2.connect(**DB_CONFIG)
    
    try:
        compute_all_percentiles(conn, dry_run=args.dry_run, single_comid=args.comid)
        if not args.dry_run:
            show_summary(conn)
    finally:
        conn.close()


if __name__ == '__main__':
    main()
