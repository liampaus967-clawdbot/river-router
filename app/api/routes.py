"""
API Routes for River Router.

Endpoints:
- POST /route - Compute route between put-in and take-out
- GET /snap - Snap a coordinate to nearest river reach
- GET /reach/{comid} - Get current conditions for a reach
"""

from fastapi import APIRouter, HTTPException, Query

from app.api.schemas import (
    RouteRequest,
    RouteResponse,
    SnapResponse,
    ReachConditions,
)

router = APIRouter(tags=["routing"])


@router.post("/route", response_model=RouteResponse)
async def compute_route(request: RouteRequest):
    """
    Compute a route between put-in and take-out points.
    
    Returns the route geometry, statistics, and elevation profile.
    """
    # TODO: Implement routing
    # 1. Snap put_in to nearest reach
    # 2. Snap take_out to nearest reach
    # 3. Run A* between nodes
    # 4. Calculate stats using NWM/EROM velocities
    # 5. Build elevation profile
    # 6. Return route GeoJSON + stats
    
    raise HTTPException(status_code=501, detail="Not implemented yet")


@router.get("/snap", response_model=SnapResponse)
async def snap_to_network(
    lat: float = Query(..., ge=-90, le=90),
    lng: float = Query(..., ge=-180, le=180),
):
    """
    Snap a coordinate to the nearest point on the river network.
    
    Returns the COMID, snap point, and reach metadata.
    """
    # TODO: Implement snapping
    # 1. Use spatial index to find nearest reach
    # 2. Project point onto reach geometry
    # 3. Return snap result with metadata
    
    raise HTTPException(status_code=501, detail="Not implemented yet")


@router.get("/reach/{comid}", response_model=ReachConditions)
async def get_reach_conditions(comid: int):
    """
    Get current flow conditions for a specific reach.
    
    Uses NWM data if available, falls back to EROM estimates.
    """
    # TODO: Implement reach lookup
    # 1. Check Redis cache for NWM data
    # 2. Fall back to EROM if no NWM
    # 3. Return conditions with source indicator
    
    raise HTTPException(status_code=501, detail="Not implemented yet")
