#!/usr/bin/env python3
"""
USGS Stream Gauge Integration for River Router
Fetches gauge locations and live readings from USGS Water Services API

Usage:
  python usgs_gauges.py populate   # Populate gauge locations database
  python usgs_gauges.py fetch      # Fetch latest readings for all gauges
  python usgs_gauges.py fetch-bbox <min_lng> <min_lat> <max_lng> <max_lat>  # Fetch for area
"""

import sys
import os
import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

# Database connection
DB_URL = os.environ.get(
    'DATABASE_URL',
    'postgresql://river_router:Pacific1ride@river-router-db.c6xmmyu04pdo.us-east-1.rds.amazonaws.com:5432/river_router'
)

# USGS Water Services API
USGS_SITE_API = "https://waterservices.usgs.gov/nwis/site/"
USGS_IV_API = "https://waterservices.usgs.gov/nwis/iv/"  # Instantaneous values

# Parameter codes we care about
PARAM_CODES = {
    '00060': 'streamflow_cfs',      # Discharge, cubic feet per second
    '00065': 'gage_height_ft',      # Gage height, feet
    '00010': 'water_temp_c',        # Water temperature, Celsius
    '00045': 'precip_in',           # Precipitation, inches
}


def get_db_connection():
    """Get database connection."""
    return psycopg2.connect(DB_URL)


def setup_tables():
    """Create USGS gauge tables if they don't exist."""
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Gauge sites table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS usgs_gauges (
            site_no VARCHAR(20) PRIMARY KEY,
            site_name TEXT,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            state_cd VARCHAR(2),
            county_cd VARCHAR(5),
            huc_cd VARCHAR(16),
            drain_area_sq_mi DOUBLE PRECISION,
            contrib_drain_area_sq_mi DOUBLE PRECISION,
            datum_ft DOUBLE PRECISION,
            geom GEOMETRY(Point, 4326),
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );
        
        CREATE INDEX IF NOT EXISTS idx_usgs_gauges_geom 
        ON usgs_gauges USING GIST (geom);
        
        CREATE INDEX IF NOT EXISTS idx_usgs_gauges_huc
        ON usgs_gauges (huc_cd);
    """)
    
    # Live readings table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS usgs_readings (
            id SERIAL PRIMARY KEY,
            site_no VARCHAR(20) REFERENCES usgs_gauges(site_no),
            reading_time TIMESTAMPTZ,
            streamflow_cfs DOUBLE PRECISION,
            gage_height_ft DOUBLE PRECISION,
            water_temp_c DOUBLE PRECISION,
            precip_in DOUBLE PRECISION,
            fetched_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(site_no, reading_time)
        );
        
        CREATE INDEX IF NOT EXISTS idx_usgs_readings_site_time
        ON usgs_readings (site_no, reading_time DESC);
    """)
    
    # Historical statistics table (for percentile comparisons)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS usgs_statistics (
            id SERIAL PRIMARY KEY,
            site_no VARCHAR(20) REFERENCES usgs_gauges(site_no),
            param_code VARCHAR(10),
            stat_type VARCHAR(20),  -- 'median', 'p10', 'p25', 'p75', 'p90', 'min', 'max'
            month INT,  -- 1-12, NULL for annual
            day INT,    -- 1-31, NULL for monthly/annual
            value DOUBLE PRECISION,
            years_of_record INT,
            begin_year INT,
            end_year INT,
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE (site_no, param_code, stat_type, month, day)
        );
        
        CREATE INDEX IF NOT EXISTS idx_usgs_statistics_site
        ON usgs_statistics (site_no);
    """)
    
    conn.commit()
    cur.close()
    conn.close()
    print("✅ Tables created/verified")


def fetch_gauge_sites(state_codes: Optional[List[str]] = None, bbox: Optional[tuple] = None):
    """
    Fetch USGS gauge site metadata.
    
    Args:
        state_codes: List of state codes (e.g., ['VT', 'NH'])
        bbox: Bounding box (min_lng, min_lat, max_lng, max_lat)
    """
    # All US states for batch processing
    ALL_STATES = [
        'AL', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 
        'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
        'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH',
        'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA',
        'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA',
        'WV', 'WI', 'WY'
    ]
    
    states_to_fetch = state_codes if state_codes else ALL_STATES
    
    all_sites = []
    
    # Process states one at a time (USGS API limit)
    for state in states_to_fetch:
        params = {
            'format': 'rdb',
            'siteType': 'ST',  # Stream
            'siteStatus': 'active',
            'hasDataTypeCd': 'iv',  # Has instantaneous values
            'stateCd': state
        }
        
        if bbox:
            params['bBox'] = f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}"
            del params['stateCd']
        
        print(f"📡 Fetching gauge sites for {state}...", end=" ", flush=True)
        
        try:
            response = requests.get(USGS_SITE_API, params=params, timeout=60)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"⚠️  Error: {e}")
            continue
        
        # Parse RDB format (tab-separated with # comments)
        headers = None
        
        for line in response.text.split('\n'):
            if line.startswith('#') or not line.strip():
                continue
            if headers is None:
                headers = line.split('\t')
                continue
            if line.startswith('5s'):  # Skip format line
                continue
                
            values = line.split('\t')
            if len(values) < len(headers):
                continue
                
            row = dict(zip(headers, values))
            
            try:
                site = {
                    'site_no': row.get('site_no', '').strip(),
                    'site_name': row.get('station_nm', '').strip(),
                    'latitude': float(row.get('dec_lat_va', 0) or 0),
                    'longitude': float(row.get('dec_long_va', 0) or 0),
                    'state_cd': row.get('state_cd', '').strip(),
                    'county_cd': row.get('county_cd', '').strip(),
                    'huc_cd': row.get('huc_cd', '').strip(),
                    'drain_area_sq_mi': float(row.get('drain_area_va', 0) or 0) or None,
                    'contrib_drain_area_sq_mi': float(row.get('contrib_drain_area_va', 0) or 0) or None,
                    'datum_ft': float(row.get('alt_va', 0) or 0) or None,
                }
                
                if site['site_no'] and site['latitude'] and site['longitude']:
                    all_sites.append(site)
            except (ValueError, KeyError) as e:
                continue
        
        state_count = len([s for s in all_sites if s.get('state_cd') == state])
        print(f"found {state_count}")
    
    print(f"📊 Found {len(all_sites)} active stream gauges total")
    return all_sites


def populate_gauges(state_codes: Optional[List[str]] = None, bbox: Optional[tuple] = None):
    """Populate the usgs_gauges table with site locations."""
    setup_tables()
    sites = fetch_gauge_sites(state_codes, bbox)
    
    if not sites:
        print("⚠️  No sites found")
        return
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Upsert sites
    sql = """
        INSERT INTO usgs_gauges (
            site_no, site_name, latitude, longitude, state_cd, county_cd,
            huc_cd, drain_area_sq_mi, contrib_drain_area_sq_mi, datum_ft, geom
        ) VALUES %s
        ON CONFLICT (site_no) DO UPDATE SET
            site_name = EXCLUDED.site_name,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            state_cd = EXCLUDED.state_cd,
            county_cd = EXCLUDED.county_cd,
            huc_cd = EXCLUDED.huc_cd,
            drain_area_sq_mi = EXCLUDED.drain_area_sq_mi,
            contrib_drain_area_sq_mi = EXCLUDED.contrib_drain_area_sq_mi,
            datum_ft = EXCLUDED.datum_ft,
            geom = EXCLUDED.geom,
            updated_at = NOW()
    """
    
    values = [
        (
            s['site_no'], s['site_name'], s['latitude'], s['longitude'],
            s['state_cd'], s['county_cd'], s['huc_cd'], s['drain_area_sq_mi'],
            s['contrib_drain_area_sq_mi'], s['datum_ft'],
            f"SRID=4326;POINT({s['longitude']} {s['latitude']})"
        )
        for s in sites
    ]
    
    execute_values(cur, sql, values, template="""(
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, ST_GeomFromEWKT(%s)
    )""")
    
    conn.commit()
    print(f"✅ Upserted {len(sites)} gauge sites")
    
    cur.close()
    conn.close()


def fetch_live_readings(site_nos: Optional[List[str]] = None, bbox: Optional[tuple] = None):
    """
    Fetch latest instantaneous values from USGS gauges.
    
    Args:
        site_nos: Specific site numbers to fetch
        bbox: Bounding box to fetch sites within
    """
    params = {
        'format': 'json',
        'parameterCd': ','.join(PARAM_CODES.keys()),
        'siteStatus': 'active',
    }
    
    if site_nos:
        # USGS limits to 100 sites per request
        all_readings = []
        for i in range(0, len(site_nos), 100):
            batch = site_nos[i:i+100]
            params['sites'] = ','.join(batch)
            readings = _fetch_iv_batch(params)
            all_readings.extend(readings)
        return all_readings
    elif bbox:
        params['bBox'] = f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}"
    else:
        # Fetch all from database
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT site_no FROM usgs_gauges")
        all_sites = [row[0] for row in cur.fetchall()]
        cur.close()
        conn.close()
        
        if not all_sites:
            print("⚠️  No sites in database. Run 'populate' first.")
            return []
        
        return fetch_live_readings(site_nos=all_sites)
    
    return _fetch_iv_batch(params)


def _fetch_iv_batch(params: Dict[str, Any]) -> List[Dict]:
    """Fetch a batch of instantaneous values."""
    print(f"📡 Fetching live readings...")
    
    try:
        response = requests.get(USGS_IV_API, params=params, timeout=120)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        print(f"⚠️  Error fetching readings: {e}")
        return []
    
    readings = {}  # site_no -> reading dict
    
    time_series = data.get('value', {}).get('timeSeries', [])
    
    for ts in time_series:
        site_no = ts.get('sourceInfo', {}).get('siteCode', [{}])[0].get('value')
        param_code = ts.get('variable', {}).get('variableCode', [{}])[0].get('value')
        
        if not site_no or param_code not in PARAM_CODES:
            continue
        
        values = ts.get('values', [{}])[0].get('value', [])
        if not values:
            continue
        
        # Get most recent value
        latest = values[-1]
        
        if site_no not in readings:
            readings[site_no] = {
                'site_no': site_no,
                'reading_time': None,
                'streamflow_cfs': None,
                'gage_height_ft': None,
                'water_temp_c': None,
                'precip_in': None,
            }
        
        param_name = PARAM_CODES[param_code]
        try:
            readings[site_no][param_name] = float(latest['value'])
            readings[site_no]['reading_time'] = latest['dateTime']
        except (ValueError, KeyError):
            pass
    
    print(f"📊 Got readings for {len(readings)} sites")
    return list(readings.values())


def store_readings(readings: List[Dict]):
    """Store readings in the database."""
    if not readings:
        return
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    sql = """
        INSERT INTO usgs_readings (
            site_no, reading_time, streamflow_cfs, gage_height_ft, 
            water_temp_c, precip_in
        ) VALUES %s
        ON CONFLICT (site_no, reading_time) DO UPDATE SET
            streamflow_cfs = EXCLUDED.streamflow_cfs,
            gage_height_ft = EXCLUDED.gage_height_ft,
            water_temp_c = EXCLUDED.water_temp_c,
            precip_in = EXCLUDED.precip_in,
            fetched_at = NOW()
    """
    
    values = [
        (
            r['site_no'], r['reading_time'], r['streamflow_cfs'],
            r['gage_height_ft'], r['water_temp_c'], r['precip_in']
        )
        for r in readings if r.get('reading_time')
    ]
    
    if values:
        execute_values(cur, sql, values)
        conn.commit()
        print(f"✅ Stored {len(values)} readings")
    
    cur.close()
    conn.close()


def fetch_statistics(site_nos: List[str]):
    """
    Fetch historical statistics for sites (for percentile comparisons).
    Uses USGS Statistics Service.
    """
    STATS_API = "https://waterservices.usgs.gov/nwis/stat/"
    
    print(f"📡 Fetching historical statistics for {len(site_nos)} sites...")
    
    all_stats = []
    
    # Process in batches
    for i in range(0, len(site_nos), 50):
        batch = site_nos[i:i+50]
        
        params = {
            'format': 'json',
            'sites': ','.join(batch),
            'statReportType': 'daily',  # Daily statistics
            'statTypeCd': 'mean,p10,p50,p90,min,max',
            'parameterCd': '00060',  # Streamflow
        }
        
        try:
            response = requests.get(STATS_API, params=params, timeout=120)
            response.raise_for_status()
            data = response.json()
            
            for ts in data.get('value', {}).get('timeSeries', []):
                site_no = ts.get('sourceInfo', {}).get('siteCode', [{}])[0].get('value')
                
                for stat in ts.get('values', [{}])[0].get('value', []):
                    try:
                        all_stats.append({
                            'site_no': site_no,
                            'param_code': '00060',
                            'stat_type': stat.get('statCd', {}).get('value'),
                            'month': int(stat.get('month', 0)) or None,
                            'day': int(stat.get('day', 0)) or None,
                            'value': float(stat.get('value', 0)),
                        })
                    except (ValueError, KeyError):
                        pass
                        
        except requests.exceptions.RequestException as e:
            print(f"⚠️  Error fetching stats batch: {e}")
    
    print(f"📊 Got {len(all_stats)} statistic records")
    return all_stats


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    
    command = sys.argv[1]
    
    if command == 'populate':
        # Optional: specify states as additional args
        states = sys.argv[2:] if len(sys.argv) > 2 else None
        populate_gauges(state_codes=states)
        
    elif command == 'fetch':
        readings = fetch_live_readings()
        store_readings(readings)
        
    elif command == 'fetch-bbox' and len(sys.argv) == 6:
        bbox = tuple(map(float, sys.argv[2:6]))
        readings = fetch_live_readings(bbox=bbox)
        store_readings(readings)
        
    elif command == 'setup':
        setup_tables()
        
    else:
        print(__doc__)
        sys.exit(1)


if __name__ == '__main__':
    main()
