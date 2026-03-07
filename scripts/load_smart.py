#!/usr/bin/env python3
"""Load NHDPlus - skips cells that already have data."""
import os, time, psycopg2, warnings
warnings.filterwarnings('ignore')

GRID = [(lon,lat,lon+2,lat+2) for lat in range(24,50,2) for lon in range(-126,-66,2)]

def get_db():
    with open(os.path.expanduser("~/.bashrc")) as f:
        for line in f:
            if "DATABASE_URL" in line:
                return psycopg2.connect(line.split("=",1)[1].strip().strip('"'))

def has_data(conn, bbox):
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM river_edges WHERE geom && ST_MakeEnvelope(%s,%s,%s,%s,4326) LIMIT 1", bbox)
    r = cur.fetchone()
    cur.close()
    return r is not None

def download(bbox):
    from pynhd import NHDPlusHR
    try:
        return NHDPlusHR("flowline").bygeom(bbox, geo_crs=4326)
    except:
        return None

def load(gdf, conn):
    if gdf is None or len(gdf)==0: return 0
    cur = conn.cursor()
    gdf.columns = [c.lower() for c in gdf.columns]
    n = 0
    for _, r in gdf.iterrows():
        g = r.get('geometry')
        if not g or g.is_empty: continue
        if g.geom_type == 'MultiLineString': g = g.geoms[0]
        if g.geom_type != 'LineString': continue
        cid = r.get('nhdplusid') or r.get('comid')
        if not cid: continue
        try:
            cur.execute("""INSERT INTO river_edges(comid,gnis_name,lengthkm,from_node,to_node,stream_order,slope,geom)
                VALUES(%s,%s,%s,%s,%s,%s,%s,ST_GeomFromText(%s,4326)) ON CONFLICT DO NOTHING""",
                (int(float(cid)), r.get('gnis_name'), r.get('lengthkm'),
                 int(float(r.get('fromnode'))) if r.get('fromnode') else None,
                 int(float(r.get('tonode'))) if r.get('tonode') else None,
                 r.get('streamorde'), r.get('slope'), g.wkt))
            n += 1
        except: pass
    conn.commit()
    cur.close()
    return n

conn = get_db()
cur = conn.cursor()
cur.execute("SELECT COUNT(*) FROM river_edges")
print(f"Start: {cur.fetchone()[0]:,}")
skip = new = 0
for i, bbox in enumerate(GRID):
    if has_data(conn, bbox):
        skip += 1
        if skip % 50 == 0: print(f"Skipped {skip} cells...")
        continue
    print(f"[{i+1}/390] {bbox}")
    gdf = download(bbox)
    if gdf is None: 
        print("  No data")
        continue
    load(gdf, conn)
    new += 1
    cur.execute("SELECT COUNT(*) FROM river_edges")
    print(f"  Loaded, total: {cur.fetchone()[0]:,}")
cur.execute("SELECT COUNT(*) FROM river_edges")
print(f"Done: {cur.fetchone()[0]:,} (skipped {skip}, loaded {new})")
