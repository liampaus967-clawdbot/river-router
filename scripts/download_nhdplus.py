#!/usr/bin/env python3
"""
Download NHDPlus V2 data and load into PostGIS.
Downloads region by region to manage disk space.
"""

import os
import sys
import time
import psycopg2
from psycopg2.extras import execute_values
import geopandas as gpd
from pathlib import Path

# NHDPlus V2 regions (VPUs)
REGIONS = [
    ("01", "Northeast"),
    ("02", "Mid-Atlantic"),
    ("03N", "South Atlantic North"),
    ("03S", "South Atlantic South"),
    ("03W", "South Atlantic West"),
    ("04", "Great Lakes"),
    ("05", "Ohio"),
    ("06", "Tennessee"),
    ("07", "Upper Mississippi"),
    ("08", "Lower Mississippi"),
    ("09", "Souris-Red-Rainy"),
    ("10U", "Upper Missouri"),
    ("10L", "Lower Missouri"),
    ("11", "Arkansas-White-Red"),
    ("12", "Texas-Gulf"),
    ("13", "Rio Grande"),
    ("14", "Upper Colorado"),
    ("15", "Lower Colorado"),
    ("16", "Great Basin"),
    ("17", "Pacific Northwest"),
    ("18", "California"),
]

DATA_DIR = Path(__file__).parent.parent / "data" / "nhdplus"
DATA_DIR.mkdir(parents=True, exist_ok=True)


def get_db_connection():
    """Get database connection from environment."""
    url = os.environ.get("DATABASE_URL")
    if not url:
        # Try to load from bashrc
        import subprocess
        result = subprocess.run(
            ["bash", "-c", "source ~/.bashrc && echo $DATABASE_URL"],
            capture_output=True, text=True
        )
        url = result.stdout.strip()
    
    if not url:
        raise ValueError("DATABASE_URL not set")
    
    return psycopg2.connect(url)


def create_tables(conn):
    """Create the river_edges table if it doesn't exist."""
    cur = conn.cursor()
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS river_edges (
            id SERIAL PRIMARY KEY,
            comid BIGINT UNIQUE NOT NULL,
            gnis_name VARCHAR(255),
            lengthkm FLOAT,
            from_node BIGINT,
            to_node BIGINT,
            hydroseq BIGINT,
            stream_order INT,
            slope FLOAT,
            min_elev_m FLOAT,
            max_elev_m FLOAT,
            velocity_fps FLOAT,
            flow_cfs FLOAT,
            ftype INT,
            fcode INT,
            region VARCHAR(10),
            geom GEOMETRY(LineString, 4326)
        );
        
        CREATE INDEX IF NOT EXISTS idx_river_edges_comid ON river_edges(comid);
        CREATE INDEX IF NOT EXISTS idx_river_edges_from_node ON river_edges(from_node);
        CREATE INDEX IF NOT EXISTS idx_river_edges_to_node ON river_edges(to_node);
        CREATE INDEX IF NOT EXISTS idx_river_edges_geom ON river_edges USING GIST(geom);
    """)
    
    conn.commit()
    cur.close()
    print("✅ Tables created/verified")


def download_region_pynhd(region_code):
    """Download NHDPlus data for a region using pynhd."""
    from pynhd import NHDPlusHR, NLDI, WaterData
    
    print(f"  Downloading via pynhd...")
    
    # Map region code to HUC2
    huc2 = region_code.replace("N", "").replace("S", "").replace("W", "").replace("U", "").replace("L", "")
    
    try:
        # Use WaterData for NHDPlus V2 (medium resolution, has EROM attributes)
        wd = WaterData("nhdflowline_network")
        
        # Get by HUC2
        flw = wd.byfilter(f"huc2 = '{huc2.zfill(2)}'")
        return flw
    except Exception as e:
        print(f"  WaterData failed: {e}")
        
        # Fallback: try NHDPlusHR
        try:
            nhdhr = NHDPlusHR("flowline")
            flw = nhdhr.byfilter(f"huc2 = '{huc2.zfill(2)}'")
            return flw
        except Exception as e2:
            print(f"  NHDPlusHR also failed: {e2}")
            return None


def download_region_direct(region_code):
    """Download NHDPlus V2 directly from EPA."""
    import requests
    import zipfile
    
    # NHDPlus V2 download URLs
    base_url = "https://dmap-data-commons-ow.s3.amazonaws.com/NHDPlusV21/Data/NHDPlusV21_NationalData_Seamless_Geodatabase_Lower48_07.7z"
    
    # For now, use the pynhd approach which is more reliable
    return None


def load_to_postgis(gdf, region_code, conn):
    """Load GeoDataFrame into PostGIS."""
    if gdf is None or len(gdf) == 0:
        print(f"  No data to load for region {region_code}")
        return 0
    
    cur = conn.cursor()
    
    # Standardize column names (lowercase)
    gdf.columns = [c.lower() for c in gdf.columns]
    
    # Get relevant columns
    rows = []
    for idx, row in gdf.iterrows():
        geom = row.get('geometry')
        if geom is None or geom.is_empty:
            continue
        
        # Handle MultiLineString by taking first part
        if geom.geom_type == 'MultiLineString':
            geom = geom.geoms[0]
        
        if geom.geom_type != 'LineString':
            continue
        
        comid = row.get('comid') or row.get('nhdplusid') or row.get('permanent_identifier')
        if comid is None:
            continue
        
        try:
            comid = int(float(comid))
        except:
            continue
        
        rows.append((
            comid,
            row.get('gnis_name') or row.get('gnis_nm'),
            row.get('lengthkm'),
            int(float(row.get('fromnode') or row.get('from_node') or 0)) or None,
            int(float(row.get('tonode') or row.get('to_node') or 0)) or None,
            row.get('hydroseq'),
            row.get('streamorde') or row.get('streamorder') or row.get('stream_order'),
            row.get('slope'),
            float(row.get('minelevsmo', 0) or 0) / 100 if row.get('minelevsmo') else None,  # cm to m
            float(row.get('maxelevsmo', 0) or 0) / 100 if row.get('maxelevsmo') else None,
            row.get('vema') or row.get('vama') or row.get('velocity'),  # EROM velocity ft/s
            row.get('qema') or row.get('qama') or row.get('qa'),  # EROM flow cfs
            row.get('ftype'),
            row.get('fcode'),
            region_code,
            geom.wkt
        ))
    
    if not rows:
        print(f"  No valid rows to insert for region {region_code}")
        return 0
    
    # Insert in batches
    batch_size = 5000
    inserted = 0
    
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i+batch_size]
        
        try:
            cur.executemany("""
                INSERT INTO river_edges 
                (comid, gnis_name, lengthkm, from_node, to_node, hydroseq, 
                 stream_order, slope, min_elev_m, max_elev_m, velocity_fps, 
                 flow_cfs, ftype, fcode, region, geom)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                        ST_GeomFromText(%s, 4326))
                ON CONFLICT (comid) DO NOTHING
            """, batch)
            conn.commit()
            inserted += len(batch)
        except Exception as e:
            print(f"  Error inserting batch: {e}")
            conn.rollback()
    
    cur.close()
    return inserted


def get_region_count(conn, region_code):
    """Check how many rows we have for a region."""
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM river_edges WHERE region = %s", (region_code,))
    count = cur.fetchone()[0]
    cur.close()
    return count


def main():
    print("=" * 60)
    print("NHDPlus V2 National Data Loader")
    print("=" * 60)
    
    # Connect to database
    print("\nConnecting to database...")
    conn = get_db_connection()
    print("✅ Connected")
    
    # Create tables
    create_tables(conn)
    
    # Check current status
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM river_edges")
    total = cur.fetchone()[0]
    cur.close()
    print(f"\nCurrent total: {total:,} edges in database")
    
    # Process each region
    print("\n" + "=" * 60)
    print("Downloading and loading regions...")
    print("=" * 60)
    
    for region_code, region_name in REGIONS:
        print(f"\n[{region_code}] {region_name}")
        
        # Check if already loaded
        existing = get_region_count(conn, region_code)
        if existing > 0:
            print(f"  Already loaded: {existing:,} edges. Skipping.")
            continue
        
        # Download
        start = time.time()
        gdf = download_region_pynhd(region_code)
        
        if gdf is None or len(gdf) == 0:
            print(f"  ⚠️ No data retrieved for {region_code}")
            continue
        
        print(f"  Downloaded: {len(gdf):,} features in {time.time()-start:.1f}s")
        
        # Load to PostGIS
        start = time.time()
        inserted = load_to_postgis(gdf, region_code, conn)
        print(f"  Loaded: {inserted:,} edges in {time.time()-start:.1f}s")
        
        # Free memory
        del gdf
    
    # Final count
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM river_edges")
    total = cur.fetchone()[0]
    cur.execute("SELECT COUNT(DISTINCT region) FROM river_edges")
    regions = cur.fetchone()[0]
    cur.close()
    
    print("\n" + "=" * 60)
    print(f"✅ Complete: {total:,} edges across {regions} regions")
    print("=" * 60)
    
    conn.close()


if __name__ == "__main__":
    main()
