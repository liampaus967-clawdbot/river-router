"""
Pydantic schemas for API request/response models.
"""

from typing import List, Optional
from pydantic import BaseModel, Field


# === Request Models ===

class Coordinate(BaseModel):
    """A geographic coordinate."""
    lat: float = Field(..., ge=-90, le=90, description="Latitude")
    lng: float = Field(..., ge=-180, le=180, description="Longitude")


class RouteRequest(BaseModel):
    """Request to route between two points."""
    put_in: Coordinate
    take_out: Coordinate
    paddle_speed_mph: float = Field(
        default=0.0,
        ge=0,
        le=10,
        description="Additional paddling speed in mph"
    )


class SnapRequest(BaseModel):
    """Request to snap a point to the river network."""
    lat: float = Field(..., ge=-90, le=90)
    lng: float = Field(..., ge=-180, le=180)


# === Response Models ===

class RouteStats(BaseModel):
    """Statistics for a computed route."""
    distance_mi: float
    distance_km: float
    float_time_hours: float
    paddle_time_hours: float
    elevation_drop_ft: float
    gradient_ft_per_mi: float
    avg_flow_mph: float
    waterways: List[str]
    conditions_as_of: Optional[str] = None


class ElevationPoint(BaseModel):
    """A point on the elevation profile."""
    distance_m: float
    elevation_m: float


class RouteResponse(BaseModel):
    """Response containing the computed route."""
    route: dict  # GeoJSON FeatureCollection
    stats: RouteStats
    elevation_profile: List[ElevationPoint]


class SnapResponse(BaseModel):
    """Response from snapping a point to the network."""
    comid: int
    snap_point: Coordinate
    distance_m: float
    reach_name: Optional[str] = None
    stream_order: Optional[int] = None


class ReachConditions(BaseModel):
    """Current conditions for a river reach."""
    comid: int
    name: Optional[str]
    current_flow_cfs: Optional[float]
    current_velocity_fps: Optional[float]
    source: str  # "nwm", "erom", "estimated"
    as_of: Optional[str]


class HealthResponse(BaseModel):
    """Health check response."""
    status: str
    version: str
    graph_loaded: bool = False
    nwm_fresh: bool = False
    nwm_last_update: Optional[str] = None
