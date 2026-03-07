"""
USGS Gauge API Routes for River Router.

Endpoints:
- GET /gauges - Get gauges in bounding box with latest readings
- GET /gauges/{site_no} - Get single gauge with readings
- POST /gauges/refresh - Trigger refresh of gauge readings
"""

from fastapi import APIRouter, HTTPException, Query
from typing import Optional, List
import asyncpg
import os

router = APIRouter(prefix="/gauges", tags=["gauges"])

DATABASE_URL = os.environ.get(
    'DATABASE_URL',
    'postgresql://river_router:Pacific1ride@river-router-db.c6xmmyu04pdo.us-east-1.rds.amazonaws.com:5432/river_router'
)


async def get_db():
    """Get database connection."""
    return await asyncpg.connect(DATABASE_URL)


@router.get("")
async def get_gauges(
    min_lng: float = Query(..., ge=-180, le=180),
    min_lat: float = Query(..., ge=-90, le=90),
    max_lng: float = Query(..., ge=-180, le=180),
    max_lat: float = Query(..., ge=-90, le=90),
    limit: int = Query(100, ge=1, le=500),
):
    """
    Get USGS gauges within a bounding box with latest readings.
    
    Returns GeoJSON FeatureCollection.
    """
    conn = await get_db()
    
    try:
        rows = await conn.fetch("""
            SELECT 
                g.site_no,
                g.site_name,
                g.latitude,
                g.longitude,
                g.state_cd,
                g.drain_area_sq_mi,
                r.streamflow_cfs,
                r.gage_height_ft,
                r.water_temp_c,
                r.reading_time
            FROM usgs_gauges g
            LEFT JOIN LATERAL (
                SELECT streamflow_cfs, gage_height_ft, water_temp_c, reading_time
                FROM usgs_readings
                WHERE site_no = g.site_no
                ORDER BY reading_time DESC
                LIMIT 1
            ) r ON true
            WHERE g.geom && ST_MakeEnvelope($1, $2, $3, $4, 4326)
            ORDER BY g.drain_area_sq_mi DESC NULLS LAST
            LIMIT $5
        """, min_lng, min_lat, max_lng, max_lat, limit)
        
        features = []
        for row in rows:
            # Filter out bad data (-999999 is USGS sentinel for missing)
            streamflow = row['streamflow_cfs'] if row['streamflow_cfs'] and row['streamflow_cfs'] > -999 else None
            
            features.append({
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [row['longitude'], row['latitude']]
                },
                "properties": {
                    "site_no": row['site_no'],
                    "site_name": row['site_name'],
                    "state_cd": row['state_cd'],
                    "drain_area_sq_mi": row['drain_area_sq_mi'],
                    "streamflow_cfs": streamflow,
                    "gage_height_ft": row['gage_height_ft'],
                    "water_temp_c": row['water_temp_c'],
                    "reading_time": row['reading_time'].isoformat() if row['reading_time'] else None,
                    "usgs_url": f"https://waterdata.usgs.gov/monitoring-location/{row['site_no']}/"
                }
            })
        
        return {
            "type": "FeatureCollection",
            "features": features,
            "count": len(features),
            "bbox": [min_lng, min_lat, max_lng, max_lat]
        }
        
    finally:
        await conn.close()


@router.get("/{site_no}")
async def get_gauge(site_no: str):
    """
    Get a single USGS gauge with latest reading and recent history.
    """
    conn = await get_db()
    
    try:
        # Get gauge info
        gauge = await conn.fetchrow("""
            SELECT 
                site_no, site_name, latitude, longitude, state_cd,
                drain_area_sq_mi, datum_ft, huc_cd
            FROM usgs_gauges
            WHERE site_no = $1
        """, site_no)
        
        if not gauge:
            raise HTTPException(status_code=404, detail="Gauge not found")
        
        # Get recent readings (last 24 hours)
        readings = await conn.fetch("""
            SELECT 
                reading_time, streamflow_cfs, gage_height_ft, water_temp_c
            FROM usgs_readings
            WHERE site_no = $1
            ORDER BY reading_time DESC
            LIMIT 96  -- 24 hours at 15-min intervals
        """, site_no)
        
        return {
            "gauge": {
                "site_no": gauge['site_no'],
                "site_name": gauge['site_name'],
                "latitude": gauge['latitude'],
                "longitude": gauge['longitude'],
                "state_cd": gauge['state_cd'],
                "drain_area_sq_mi": gauge['drain_area_sq_mi'],
                "datum_ft": gauge['datum_ft'],
                "huc_cd": gauge['huc_cd'],
                "usgs_url": f"https://waterdata.usgs.gov/monitoring-location/{site_no}/"
            },
            "latest": {
                "reading_time": readings[0]['reading_time'].isoformat() if readings else None,
                "streamflow_cfs": readings[0]['streamflow_cfs'] if readings else None,
                "gage_height_ft": readings[0]['gage_height_ft'] if readings else None,
                "water_temp_c": readings[0]['water_temp_c'] if readings else None,
            } if readings else None,
            "history": [
                {
                    "time": r['reading_time'].isoformat(),
                    "flow": r['streamflow_cfs'] if r['streamflow_cfs'] and r['streamflow_cfs'] > -999 else None,
                    "height": r['gage_height_ft'],
                    "temp": r['water_temp_c']
                }
                for r in readings
            ]
        }
        
    finally:
        await conn.close()


@router.get("/stats/summary")
async def get_gauge_stats():
    """
    Get summary statistics about gauge data.
    """
    conn = await get_db()
    
    try:
        stats = await conn.fetchrow("""
            SELECT 
                (SELECT COUNT(*) FROM usgs_gauges) as total_gauges,
                (SELECT COUNT(DISTINCT site_no) FROM usgs_readings) as gauges_with_data,
                (SELECT COUNT(*) FROM usgs_readings) as total_readings,
                (SELECT MAX(fetched_at) FROM usgs_readings) as last_refresh,
                (SELECT COUNT(*) FROM usgs_readings WHERE streamflow_cfs IS NOT NULL AND streamflow_cfs > -999) as valid_flow_readings
        """)
        
        return {
            "total_gauges": stats['total_gauges'],
            "gauges_with_data": stats['gauges_with_data'],
            "total_readings": stats['total_readings'],
            "valid_flow_readings": stats['valid_flow_readings'],
            "last_refresh": stats['last_refresh'].isoformat() if stats['last_refresh'] else None,
            "refresh_interval_minutes": 15
        }
        
    finally:
        await conn.close()
