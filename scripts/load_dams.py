#!/usr/bin/env python3
"""
Load National Inventory of Dams into PostgreSQL
"""

import csv
import psycopg2
from psycopg2.extras import execute_values
import os

DB_URL = os.environ.get(
    'DATABASE_URL',
    'postgresql://river_router:Pacific1ride@river-router-db.c6xmmyu04pdo.us-east-1.rds.amazonaws.com:5432/river_router'
)

def setup_table():
    """Create dams table."""
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    
    cur.execute("""
        DROP TABLE IF EXISTS hazards_dams;
        
        CREATE TABLE hazards_dams (
            id SERIAL PRIMARY KEY,
            nid_id VARCHAR(20) UNIQUE,
            dam_name TEXT,
            other_names TEXT,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            state VARCHAR(50),
            county VARCHAR(100),
            city VARCHAR(100),
            river_name TEXT,
            owner_type VARCHAR(50),
            primary_purpose VARCHAR(100),
            dam_type VARCHAR(50),
            dam_height_ft DOUBLE PRECISION,
            nid_storage_acre_ft DOUBLE PRECISION,
            hazard_potential VARCHAR(20),  -- High, Significant, Low, Undetermined
            condition_assessment VARCHAR(50),
            year_completed INT,
            last_inspection DATE,
            geom GEOMETRY(Point, 4326),
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        
        CREATE INDEX idx_hazards_dams_geom ON hazards_dams USING GIST (geom);
        CREATE INDEX idx_hazards_dams_hazard ON hazards_dams (hazard_potential);
        CREATE INDEX idx_hazards_dams_state ON hazards_dams (state);
    """)
    
    conn.commit()
    cur.close()
    conn.close()
    print("✅ Table created")


def load_dams(csv_path: str):
    """Load dams from CSV."""
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    
    dams_dict = {}  # Use dict to dedupe by NID ID
    skipped = 0
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        # Skip the first line (update date)
        next(f)
        reader = csv.DictReader(f)
        
        for row in reader:
            try:
                lat = float(row.get('Latitude', 0) or 0)
                lng = float(row.get('Longitude', 0) or 0)
                
                if lat == 0 or lng == 0:
                    skipped += 1
                    continue
                
                # Parse year
                year = None
                year_str = row.get('Year Completed', '')
                if year_str and year_str.isdigit():
                    year = int(year_str)
                
                # Parse height
                height = None
                height_str = row.get('Dam Height (Ft)', '')
                try:
                    height = float(height_str) if height_str else None
                except:
                    pass
                
                # Parse storage
                storage = None
                storage_str = row.get('NID Storage (Acre-Ft)', '')
                try:
                    storage = float(storage_str) if storage_str else None
                except:
                    pass
                
                nid_id = row.get('NID ID', '')
                if nid_id:  # Only add if has valid ID
                    dams_dict[nid_id] = (
                        nid_id,
                        row.get('Dam Name', ''),
                        row.get('Other Names', ''),
                        lat,
                        lng,
                        row.get('State', ''),
                        row.get('County', ''),
                        row.get('City', ''),
                        row.get('River or Stream Name', ''),
                        row.get('Primary Owner Type', ''),
                        row.get('Primary Purpose', ''),
                        row.get('Primary Dam Type', ''),
                        height,
                        storage,
                        row.get('Hazard Potential Classification', ''),
                        row.get('Condition Assessment', ''),
                        year,
                    )
                
            except Exception as e:
                skipped += 1
                continue
    
    dams = list(dams_dict.values())
    print(f"📊 Parsed {len(dams)} unique dams, skipped {skipped}")
    
    # Bulk insert
    sql = """
        INSERT INTO hazards_dams (
            nid_id, dam_name, other_names, latitude, longitude,
            state, county, city, river_name, owner_type,
            primary_purpose, dam_type, dam_height_ft, nid_storage_acre_ft,
            hazard_potential, condition_assessment, year_completed, geom
        ) VALUES %s
        ON CONFLICT (nid_id) DO UPDATE SET
            dam_name = EXCLUDED.dam_name,
            hazard_potential = EXCLUDED.hazard_potential,
            condition_assessment = EXCLUDED.condition_assessment
    """
    
    template = """(
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        ST_SetSRID(ST_MakePoint(%s, %s), 4326)
    )"""
    
    # Add lng, lat at end for geometry
    values = [d + (d[4], d[3]) for d in dams]
    
    execute_values(cur, sql, values, template=template, page_size=1000)
    conn.commit()
    
    # Get counts by hazard level
    cur.execute("""
        SELECT hazard_potential, COUNT(*) 
        FROM hazards_dams 
        GROUP BY hazard_potential 
        ORDER BY COUNT(*) DESC
    """)
    
    print("\n📊 Dams by hazard level:")
    for row in cur.fetchall():
        print(f"  {row[0] or 'Unknown'}: {row[1]:,}")
    
    cur.close()
    conn.close()
    print(f"\n✅ Loaded {len(dams)} dams into database")


if __name__ == '__main__':
    setup_table()
    load_dams('/home/ubuntu/river-router-api/data/hazards/nid_dams.csv')
