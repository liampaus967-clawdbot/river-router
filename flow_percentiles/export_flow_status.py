#!/usr/bin/env python3
"""
Export current flow status as GeoJSON for map visualization.

Usage:
    python export_flow_status.py                    # outputs flow_status.geojson
    python export_flow_status.py -o my_output.geojson
"""

import argparse
import json
import os
from pathlib import Path
from datetime import datetime

import psycopg2
from psycopg2.extras import RealDictCursor

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

QUERY = """
SELECT 
    rv.comid,
    rv.gnis_name,
    ROUND(rv.streamflow_cms::numeric, 3) as current_flow_cms,
    ROUND(fp.flow_p10::numeric, 3) as flow_p10,
    ROUND(fp.flow_p25::numeric, 3) as flow_p25,
    ROUND(fp.flow_p50::numeric, 3) as flow_p50,
    ROUND(fp.flow_p75::numeric, 3) as flow_p75,
    ROUND(fp.flow_p90::numeric, 3) as flow_p90,
    CASE 
        WHEN rv.streamflow_cms IS NULL THEN 'no_data'
        WHEN rv.streamflow_cms <= fp.flow_p10 THEN 'very_low'
        WHEN rv.streamflow_cms <= fp.flow_p25 THEN 'low'
        WHEN rv.streamflow_cms <= fp.flow_p75 THEN 'normal'
        WHEN rv.streamflow_cms <= fp.flow_p90 THEN 'high'
        ELSE 'very_high'
    END as flow_status,
    fp.week_of_year,
    ST_AsGeoJSON(rv.geom)::json as geometry
FROM river_velocities rv
JOIN flow_percentiles fp 
    ON rv.comid = fp.comid 
    AND fp.week_of_year = EXTRACT(WEEK FROM NOW())::INT
WHERE rv.geom IS NOT NULL;
"""

def export_geojson(output_path: str):
    print(f"Connecting to database...")
    conn = psycopg2.connect(**DB_CONFIG)
    
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            print(f"Querying current flow status (week {datetime.now().isocalendar()[1]})...")
            cur.execute(QUERY)
            rows = cur.fetchall()
        
        print(f"Found {len(rows):,} features")
        
        # Build GeoJSON
        features = []
        for row in rows:
            geom = row.pop('geometry')
            features.append({
                "type": "Feature",
                "properties": dict(row),
                "geometry": geom
            })
        
        geojson = {
            "type": "FeatureCollection",
            "generated": datetime.now().isoformat(),
            "features": features
        }
        
        with open(output_path, 'w') as f:
            json.dump(geojson, f)
        
        print(f"✅ Exported to {output_path}")
        
        # Summary
        status_counts = {}
        for row in rows:
            s = row.get('flow_status', 'unknown')
            status_counts[s] = status_counts.get(s, 0) + 1
        
        print(f"\nFlow status summary:")
        for status, count in sorted(status_counts.items()):
            print(f"  {status}: {count:,}")
        
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description='Export flow status GeoJSON')
    parser.add_argument('-o', '--output', default='flow_status.geojson', help='Output file path')
    args = parser.parse_args()
    
    export_geojson(args.output)


if __name__ == '__main__':
    main()
