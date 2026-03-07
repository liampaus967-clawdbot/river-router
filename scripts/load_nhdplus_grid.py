#!/usr/bin/env python3
"""
Load NHDPlus data into PostGIS by downloading in grid cells.
Uses pynhd which we know works reliably.
"""

import os
import sys
import time
import psycopg2
from psycopg2.extras import execute_batch
import warnings
warnings.filterwarnings('ignore')

# Grid cells covering CONUS (2-degree cells)
# This gives us manageable chunks that pynhd can handle
GRID_CELLS = []
for lat in range(24, 50, 2):  # 24°N to 50°N
    for lon in range(-126, -66, 2):  # 126°W to 66°W
        GRID_CELLS.append((lon, lat, lon+2, lat+2))

print(f"Total grid cells: {len(GRID_CELLS)}")


def get_db_connection():
    """Get database connection."""
    url = os.environ.get("DATABASE_URL")
    if not url:
        # Read from bashrc
        bashrc = os.path.expanduser("~/.bashrc")
        if os.path.exists(bashrc):
            with open(bashrc) as f:
                for line in f:
                    if "DATABASE_URL" in line and "=" in line:
                        # Extract URL from: export DATABASE_URL="..."
                        url = line.split("=", 1)[1].strip().strip('"').strip("'")
                        break
    if not url:
        raise ValueError("DATABASE_URL not set")
    return psycopg2.connect(url)


def create_tables(conn):
    """Create tables if needed."""
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
            geom GEOMETRY(LineString, 4326)
        );
        
        CREATE INDEX IF NOT EXISTS idx_river_edges_comid ON river_edges(comid);
        CREATE INDEX IF NOT EXISTS idx_river_edges_from_node ON river_edges(from_node);
        CREATE INDEX IF NOT EXISTS idx_river_edges_to_node ON river_edges(to_node);
        CREATE INDEX IF NOT EXISTS idx_river_edges_geom ON river_edges USING GIST(geom);
    """)
    conn.commit()
    cur.close()


def download_cell(bbox):
    """Download flowlines for a grid cell."""
    from pynhd import NHDPlusHR
    
    try:
        nhdhr = NHDPlusHR("flowline")
        flw = nhdhr.bygeom(bbox, geo_crs=4326)
        return flw
    except Exception as e:
        print(f"    Error: {e}")
        return None


def load_to_db(gdf, conn):
    """Load geodataframe to database."""
    if gdf is None or len(gdf) == 0:
        return 0
    
    cur = conn.cursor()
    gdf.columns = [c.lower() for c in gdf.columns]
    
    rows = []
    for idx, row in gdf.iterrows():
        geom = row.get('geometry')
        if geom is None or geom.is_empty:
            continue
        
        if geom.geom_type == 'MultiLineString':
            geom = geom.geoms[0]
        if geom.geom_type != 'LineString':
            continue
        
        comid = row.get('nhdplusid') or row.get('comid')
        if comid is None:
            continue
        try:
            comid = int(float(comid))
        except:
            continue
        
        from_node = row.get('fromnode')
        to_node = row.get('tonode')
        
        rows.append((
            comid,
            row.get('gnis_name'),
            row.get('lengthkm'),
            int(float(from_node)) if from_node and str(from_node) != 'nan' else None,
            int(float(to_node)) if to_node and str(to_node) != 'nan' else None,
            row.get('hydroseq'),
            row.get('streamorde'),
            row.get('slope'),
            float(row.get('minelevsmo', 0) or 0) / 100 if row.get('minelevsmo') else None,
            float(row.get('maxelevsmo', 0) or 0) / 100 if row.get('maxelevsmo') else None,
            row.get('vema') or row.get('vama'),
            row.get('qema') or row.get('qama'),
            row.get('ftype'),
            row.get('fcode'),
            geom.wkt
        ))
    
    if not rows:
        return 0
    
    inserted = 0
    for i in range(0, len(rows), 1000):
        batch = rows[i:i+1000]
        try:
            cur.executemany("""
                INSERT INTO river_edges 
                (comid, gnis_name, lengthkm, from_node, to_node, hydroseq,
                 stream_order, slope, min_elev_m, max_elev_m, velocity_fps,
                 flow_cfs, ftype, fcode, geom)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,ST_GeomFromText(%s,4326))
                ON CONFLICT (comid) DO NOTHING
            """, batch)
            conn.commit()
            inserted += cur.rowcount
        except Exception as e:
            print(f"    Insert error: {e}")
            conn.rollback()
    
    cur.close()
    return inserted


def get_current_count(conn):
    """Get current row count."""
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM river_edges")
    count = cur.fetchone()[0]
    cur.close()
    return count


def main():
    import sys
    
    # Allow starting from a specific cell via command line arg
    start_cell = 0
    if len(sys.argv) > 1:
        try:
            start_cell = int(sys.argv[1])
            print(f"Resuming from cell {start_cell}")
        except ValueError:
            pass
    
    print("=" * 60)
    print("NHDPlus Grid Loader")
    print("=" * 60)
    
    conn = get_db_connection()
    create_tables(conn)
    
    initial_count = get_current_count(conn)
    print(f"Starting count: {initial_count:,} edges")
    
    total_cells = len(GRID_CELLS)
    
    for i, bbox in enumerate(GRID_CELLS):
        if i < start_cell:
            continue  # Skip cells before start_cell
        print(f"\n[{i+1}/{total_cells}] Cell: {bbox}")
        
        start = time.time()
        gdf = download_cell(bbox)
        
        if gdf is None or len(gdf) == 0:
            print(f"    No data (land or ocean)")
            continue
        
        dl_time = time.time() - start
        print(f"    Downloaded: {len(gdf):,} features in {dl_time:.1f}s")
        
        start = time.time()
        inserted = load_to_db(gdf, conn)
        load_time = time.time() - start
        print(f"    Inserted: {inserted:,} new edges in {load_time:.1f}s")
        
        del gdf
        
        # Progress
        current = get_current_count(conn)
        print(f"    Total now: {current:,} edges")
    
    final_count = get_current_count(conn)
    print("\n" + "=" * 60)
    print(f"Complete: {final_count:,} edges ({final_count - initial_count:,} new)")
    print("=" * 60)
    
    conn.close()


if __name__ == "__main__":
    main()
