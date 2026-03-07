# River Router - Technical Specification

**Version:** 0.1.0  
**Last Updated:** February 2025  
**Repository:** [liampaus967-clawdbot/river-router](https://github.com/liampaus967-clawdbot/river-router)

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Data Sources](#2-data-sources)
3. [Database Schema](#3-database-schema)
4. [API Endpoints](#4-api-endpoints)
5. [Routing Algorithm](#5-routing-algorithm)
6. [Float Time Calculation](#6-float-time-calculation)
7. [Frontend](#7-frontend)
8. [Deployment](#8-deployment)
9. [Future Enhancements](#9-future-enhancements)

---

## 1. Architecture Overview

River Router is a national-scale river routing engine that computes paddling routes between any two points on the US river network, with real-time float time estimates based on hydrological data.

### System Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              FRONTEND                                        │
│  Mapbox GL JS · Click-to-route · Elevation Profile · Paddle Speed Slider   │
│  Client-side Dijkstra routing (prototype) / API calls (production)          │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 │ REST API (JSON)
┌────────────────────────────────▼────────────────────────────────────────────┐
│                            ROUTING API                                       │
│  FastAPI · A*/Dijkstra routing · NWM integration · Sub-second response     │
│  Endpoints: /route, /snap, /reach/{comid}                                   │
└─────────┬──────────────────────┬──────────────────────────┬─────────────────┘
          │                      │                          │
┌─────────▼────────┐   ┌─────────▼────────┐    ┌───────────▼──────────────────┐
│    PostGIS       │   │   Graph Cache    │    │        NWM Cache             │
│    (AWS RDS)     │   │   (In-Memory)    │    │        (Redis)               │
│                  │   │                  │    │                              │
│  • river_edges   │   │  • NetworkX      │    │  • Real-time velocities      │
│  • nwm_velocity  │   │  • Adjacency     │    │  • Hourly refresh            │
│  • ~2.96M edges  │   │    lists         │    │  • ~2.45M reach records      │
└──────────────────┘   └──────────────────┘    └──────────────────────────────┘
          │                                                  ▲
          │                                                  │
┌─────────▼──────────────────────────────────────────────────┴────────────────┐
│                           DATA INGEST PIPELINE                               │
│  pynhd → NHDPlus V2 download → PostGIS load                                 │
│  S3 → NWM NetCDF download → Parse → PostgreSQL/Redis                        │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Component Responsibilities

| Component | Technology | Responsibility |
|-----------|------------|----------------|
| **API Server** | FastAPI + Uvicorn | REST endpoints, request handling, response formatting |
| **Graph Engine** | NetworkX | In-memory graph representation, pathfinding algorithms |
| **Spatial Database** | PostgreSQL 16 + PostGIS 3.4 | River network storage, spatial queries, point snapping |
| **Velocity Cache** | Redis 7 | Real-time NWM velocity data, TTL-based expiry |
| **Data Ingest** | Python scripts + pynhd | NHDPlus download, NWM hourly ingest |
| **Frontend** | Mapbox GL JS + Astro | Map visualization, route display, user interaction |

### Data Flow

1. **User clicks put-in/take-out** → Frontend captures coordinates
2. **Snap to network** → API finds nearest river reach using PostGIS spatial index
3. **Route computation** → A*/Dijkstra traverses in-memory graph
4. **Stats calculation** → Velocity from NWM (real-time) or EROM (static fallback)
5. **Response** → GeoJSON route + statistics + elevation profile

---

## 2. Data Sources

### 2.1 NHDPlus V2 (National Hydrography Dataset Plus)

**Source:** USGS/EPA via [pynhd](https://github.com/hyriver/pynhd) library  
**Update Frequency:** Static baseline (updated ~annually by USGS)  
**Coverage:** Continental United States (CONUS)  
**Total Reaches:** ~2.7 million flowlines nationally

#### Key Attributes Used

| Field | Description | Usage |
|-------|-------------|-------|
| `comid` | Common identifier | Primary key, joins to NWM |
| `from_node` / `to_node` | Network topology | Graph edge construction |
| `lengthkm` | Reach length in km | Distance calculations |
| `gnis_name` | Geographic name | Display on route stats |
| `stream_order` | Strahler stream order (1-10) | Filtering, display sizing |
| `slope` | Reach slope | Velocity estimation fallback |
| `minelevsmo` / `maxelevsmo` | Smoothed elevation (cm) | Elevation profile |
| `va_ma` / `qa_ma` | Mean annual velocity/flow | EROM velocity fallback |
| `ftype` / `fcode` | Feature type codes | Filtering (rivers vs canals) |
| `geometry` | LineString (WGS84) | Route display, snapping |

#### Feature Type Codes (ftype)

| Code | Description | Included? |
|------|-------------|-----------|
| 460 | StreamRiver | ✅ Yes |
| 558 | ArtificialPath | ✅ Yes (through lakes) |
| 336 | CanalDitch | ⚠️ Conditional |
| 428 | Pipeline | ❌ No |
| 566 | Coastline | ❌ No |

### 2.2 National Water Model (NWM)

**Source:** NOAA via S3 bucket `s3://noaa-nwm-pds`  
**Update Frequency:** Hourly (analysis_assim product)  
**Format:** NetCDF4 (`.nc`)  
**Coverage:** All NHDPlus reaches with streamflow

#### NWM Products Used

| Product | Timing | Use Case |
|---------|--------|----------|
| `analysis_assim` | T-0 (nowcast) | Current conditions |
| `short_range` | T+1 to T+18h | Near-term forecast (future) |

#### Key Variables

| Variable | Units | Description |
|----------|-------|-------------|
| `feature_id` | — | COMID (joins to NHDPlus) |
| `velocity` | m/s | Channel velocity |
| `streamflow` | m³/s (CMS) | Discharge |

#### NWM File Naming Convention

```
s3://noaa-nwm-pds/nwm.{YYYYMMDD}/analysis_assim/nwm.t{HH}z.analysis_assim.channel_rt.tm00.conus.nc
```

Example: `nwm.20250204/analysis_assim/nwm.t12z.analysis_assim.channel_rt.tm00.conus.nc`

### 2.3 EROM (Enhanced Runoff Method)

**Source:** Embedded in NHDPlus V2 VAA (Value Added Attributes)  
**Update Frequency:** Static (seasonal/monthly means)  
**Fallback Role:** Used when NWM data unavailable or stale

#### EROM Velocity Fields

| Field | Month | Units |
|-------|-------|-------|
| `va_ma` | Annual mean | ft/s |
| `vb_ma` | January | ft/s |
| `vc_ma` | February | ft/s |
| ... | ... | ... |
| `vm_ma` | December | ft/s |

---

## 3. Database Schema

### 3.1 PostGIS Database

**Host:** `river-router-db.c6xmmyu04pdo.us-east-1.rds.amazonaws.com`  
**Engine:** PostgreSQL 16 + PostGIS 3.4  
**Database:** `river_router`

### 3.2 Tables

#### `river_edges` — River Network Reaches

Primary storage for NHDPlus flowline data with topology.

```sql
CREATE TABLE river_edges (
    id           SERIAL PRIMARY KEY,
    comid        BIGINT UNIQUE NOT NULL,      -- NHDPlus Common ID (joins to NWM)
    gnis_name    VARCHAR(255),                 -- Stream name
    lengthkm     DOUBLE PRECISION,             -- Length in kilometers
    from_node    BIGINT,                       -- Upstream node ID
    to_node      BIGINT,                       -- Downstream node ID
    hydroseq     BIGINT,                       -- Hydrological sequence
    stream_order INTEGER,                      -- Strahler order (1-10)
    slope        DOUBLE PRECISION,             -- Reach slope (m/m)
    min_elev_m   DOUBLE PRECISION,             -- Downstream elevation (meters)
    max_elev_m   DOUBLE PRECISION,             -- Upstream elevation (meters)
    velocity_fps DOUBLE PRECISION,             -- EROM velocity (ft/s)
    flow_cfs     DOUBLE PRECISION,             -- EROM flow (cfs)
    ftype        INTEGER,                      -- Feature type code
    fcode        INTEGER,                      -- Feature code
    region       VARCHAR(10),                  -- HUC2 region code
    geom         GEOMETRY(LineString, 4326)    -- WGS84 geometry
);

-- Indexes
CREATE INDEX idx_river_edges_comid ON river_edges(comid);
CREATE INDEX idx_river_edges_from_node ON river_edges(from_node);
CREATE INDEX idx_river_edges_to_node ON river_edges(to_node);
CREATE INDEX idx_river_edges_geom ON river_edges USING GIST(geom);
```

**Current Statistics:**
- Total rows: **2,956,380 edges**
- With EROM velocity: **2,637,349** (89%)
- With GNIS name: **1,351,668** (46%)
- Storage size: ~1.8 GB

**Stream Order Distribution:**

| Order | Count | Description |
|-------|-------|-------------|
| 1 | 1,495,623 | Headwater streams |
| 2 | 661,511 | Small tributaries |
| 3 | 353,216 | Medium streams |
| 4 | 199,625 | Rivers |
| 5 | 112,231 | Large rivers |
| 6+ | 110,589 | Major rivers |

#### `nwm_velocity` — Real-Time NWM Data

Stores current velocity/flow from National Water Model.

```sql
CREATE TABLE nwm_velocity (
    comid          BIGINT PRIMARY KEY,         -- Joins to river_edges
    velocity_ms    DOUBLE PRECISION,           -- Current velocity (m/s)
    streamflow_cms DOUBLE PRECISION,           -- Current flow (m³/s)
    updated_at     TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE INDEX idx_nwm_velocity_updated ON nwm_velocity(updated_at);
```

**Current Statistics:**
- Total rows: **2,449,098 reaches** with flow data
- Storage size: ~280 MB
- Update frequency: Hourly (via cron)

### 3.3 Entity Relationship

```
┌──────────────┐          ┌──────────────┐
│ river_edges  │          │ nwm_velocity │
├──────────────┤          ├──────────────┤
│ comid (PK)   │◄────────►│ comid (PK)   │
│ from_node    │          │ velocity_ms  │
│ to_node      │          │ streamflow   │
│ velocity_fps │          │ updated_at   │
│ geom         │          └──────────────┘
│ ...          │
└──────────────┘
       │
       │ from_node / to_node
       ▼
┌──────────────────────────────────────┐
│         GRAPH STRUCTURE              │
│   Nodes: from_node, to_node values   │
│   Edges: comid + attributes          │
└──────────────────────────────────────┘
```

---

## 4. API Endpoints

### 4.1 Base URL

- **Development:** `http://localhost:8000`
- **Production:** TBD (e.g., `https://api.paddleways.com/v1`)

### 4.2 Endpoints

#### `POST /route` — Compute Route

Computes a route between put-in and take-out points on the river network.

**Request Body:**

```json
{
  "put_in": {
    "lat": 44.337,
    "lng": -72.756
  },
  "take_out": {
    "lat": 44.395,
    "lng": -72.614
  },
  "paddle_speed_mph": 2.0
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `put_in` | Coordinate | ✅ | Starting point (latitude, longitude) |
| `take_out` | Coordinate | ✅ | Ending point (latitude, longitude) |
| `paddle_speed_mph` | float | ❌ | Additional paddle speed (0-10 mph, default 0) |

**Response:**

```json
{
  "route": {
    "type": "FeatureCollection",
    "features": [
      {
        "type": "Feature",
        "geometry": {
          "type": "LineString",
          "coordinates": [[-72.756, 44.337], [-72.754, 44.339], ...]
        },
        "properties": {
          "comid": 4587234,
          "name": "Winooski River",
          "stream_order": 5
        }
      }
    ]
  },
  "stats": {
    "distance_mi": 12.7,
    "distance_km": 20.4,
    "float_time_hours": 4.2,
    "paddle_time_hours": 2.8,
    "elevation_drop_ft": 82,
    "gradient_ft_per_mi": 6.5,
    "avg_flow_mph": 1.1,
    "waterways": ["Winooski River"],
    "conditions_as_of": "2025-02-04T12:00:00Z"
  },
  "elevation_profile": [
    {"distance_m": 0, "elevation_m": 185.2},
    {"distance_m": 500, "elevation_m": 184.8},
    {"distance_m": 1000, "elevation_m": 184.3}
  ]
}
```

#### `GET /snap` — Snap to Network

Snaps a geographic coordinate to the nearest point on the river network.

**Query Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `lat` | float | ✅ | Latitude (-90 to 90) |
| `lng` | float | ✅ | Longitude (-180 to 180) |

**Example:** `GET /snap?lat=44.337&lng=-72.756`

**Response:**

```json
{
  "comid": 4587234,
  "snap_point": {
    "lat": 44.3372,
    "lng": -72.7558
  },
  "distance_m": 45.2,
  "reach_name": "Winooski River",
  "stream_order": 5
}
```

#### `GET /reach/{comid}` — Reach Conditions

Returns current flow conditions for a specific river reach.

**Path Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `comid` | int | NHDPlus COMID |

**Example:** `GET /reach/4587234`

**Response:**

```json
{
  "comid": 4587234,
  "name": "Winooski River",
  "current_flow_cfs": 1240,
  "current_velocity_fps": 2.1,
  "source": "nwm",
  "as_of": "2025-02-04T12:00:00Z"
}
```

| `source` Value | Meaning |
|----------------|---------|
| `nwm` | Real-time National Water Model data |
| `erom` | EROM static mean annual velocity |
| `estimated` | Manning's equation estimate from slope |

#### `GET /health` — Health Check

**Response:**

```json
{
  "status": "healthy",
  "version": "0.1.0",
  "graph_loaded": true,
  "nwm_fresh": true,
  "nwm_last_update": "2025-02-04T12:00:00Z"
}
```

---

## 5. Routing Algorithm

### 5.1 Algorithm Selection

| Algorithm | Time Complexity | Use Case |
|-----------|-----------------|----------|
| **Dijkstra** | O((V + E) log V) | Current implementation |
| **A*** | O(E) with good heuristic | Planned (faster with geographic heuristic) |
| **Bidirectional A*** | O(√E) average | Future optimization |

### 5.2 Graph Construction

The river network is modeled as a **directed graph**:

- **Nodes:** Unique `from_node` and `to_node` values from NHDPlus topology
- **Edges:** River reaches (COMIDs) connecting nodes
- **Edge Weight:** Length in meters (for shortest path) or time (for fastest path)

```python
# Graph construction (simplified)
import networkx as nx

G = nx.DiGraph()

for edge in river_edges:
    G.add_edge(
        edge.from_node,
        edge.to_node,
        comid=edge.comid,
        length_m=edge.lengthkm * 1000,
        velocity_ms=get_velocity(edge),
        geometry=edge.geom,
        name=edge.gnis_name
    )
```

### 5.3 Bidirectional Support

Rivers flow downstream, but paddlers can travel both directions:

1. **Downstream (with flow):** Natural graph direction
2. **Upstream (against flow):** Reverse edges added to create undirected graph

```python
# For paddling, treat as undirected
G_undirected = G.to_undirected()
path = nx.shortest_path(G_undirected, start_node, end_node, weight='length_m')
```

### 5.4 Point-to-Network Snapping

Before routing, user click coordinates must be snapped to graph nodes:

```python
def snap_to_network(lng, lat, G):
    """Find nearest node using Euclidean distance."""
    best_node = None
    best_dist = float('inf')
    
    for node, data in G.nodes(data=True):
        dx = data['x'] - lng
        dy = data['y'] - lat
        dist = sqrt(dx**2 + dy**2)
        if dist < best_dist:
            best_dist = dist
            best_node = node
    
    # Convert degrees to meters (approximate)
    dist_m = best_dist * 111000 * cos(radians(lat))
    
    return best_node, dist_m
```

**Production Enhancement:** Use PostGIS spatial index for O(log n) snapping:

```sql
SELECT comid, from_node,
       ST_Distance(geom::geography, ST_Point(-72.756, 44.337)::geography) as dist_m
FROM river_edges
ORDER BY geom <-> ST_Point(-72.756, 44.337)
LIMIT 1;
```

### 5.5 A* Heuristic (Planned)

For faster routing, A* uses a geographic heuristic:

```python
def heuristic(node1, node2):
    """Haversine distance as admissible heuristic."""
    lat1, lon1 = G.nodes[node1]['y'], G.nodes[node1]['x']
    lat2, lon2 = G.nodes[node2]['y'], G.nodes[node2]['x']
    return haversine(lat1, lon1, lat2, lon2)

path = nx.astar_path(G, start, end, heuristic=heuristic, weight='length_m')
```

---

## 6. Float Time Calculation

### 6.1 Core Formula

```
Float Time (hours) = Σ (segment_length_m / velocity_ms) / 3600
```

For paddle time with additional paddling effort:

```
Paddle Time (hours) = Σ (segment_length_m / (velocity_ms + paddle_speed_ms)) / 3600
```

### 6.2 Velocity Source Priority

```
┌─────────────────────────────────────────┐
│         VELOCITY LOOKUP ORDER           │
├─────────────────────────────────────────┤
│ 1. NWM Real-time (if fresh < 2 hours)   │
│    ↓ fallback                           │
│ 2. EROM Mean Annual (va_ma field)       │
│    ↓ fallback                           │
│ 3. Manning's Equation Estimate          │
│    ↓ fallback                           │
│ 4. Default: 0.5 m/s (~1.1 mph)          │
└─────────────────────────────────────────┘
```

### 6.3 Velocity Sources in Detail

#### NWM Real-Time

```python
def get_nwm_velocity(comid):
    """Fetch from nwm_velocity table."""
    query = """
        SELECT velocity_ms, updated_at
        FROM nwm_velocity
        WHERE comid = %s AND updated_at > NOW() - INTERVAL '2 hours'
    """
    result = db.execute(query, (comid,))
    return result.velocity_ms if result else None
```

#### EROM Static

```python
def get_erom_velocity(row):
    """Get EROM velocity from NHDPlus attributes (ft/s → m/s)."""
    for field in ['va_ma', 'vb_ma', 'vc_ma', 'vd_ma', 've_ma']:
        val = row.get(field)
        if val and val > 0:
            return val * 0.3048  # ft/s to m/s
    return None
```

#### Manning's Equation Estimate

When no velocity data exists, estimate from slope:

```python
def estimate_velocity_manning(slope):
    """
    Manning's equation: V = (1/n) * R^(2/3) * S^(1/2)
    
    Assumptions:
    - n = 0.035 (natural stream with some vegetation)
    - R = 0.5 m (hydraulic radius for typical small river)
    """
    if slope is None or slope <= 0:
        return 0.5  # Default
    
    n = 0.035  # Manning's roughness coefficient
    R = 0.5    # Hydraulic radius (meters)
    V = (1/n) * (R ** (2/3)) * (slope ** 0.5)
    
    return min(V, 5.0)  # Cap at 5 m/s for safety
```

### 6.4 Unit Conversions

| From | To | Factor |
|------|----|--------|
| ft/s | m/s | × 0.3048 |
| mph | m/s | × 0.44704 |
| m/s | mph | × 2.23694 |
| km | miles | × 0.621371 |
| meters | feet | × 3.28084 |
| cm | m | × 0.01 |

### 6.5 Example Calculation

For a 10 km route with average velocity 0.8 m/s:

```
Distance: 10 km = 10,000 m
Velocity: 0.8 m/s
Float Time: 10,000 / 0.8 / 3600 = 3.47 hours

With +2 mph paddling (0.89 m/s):
Effective velocity: 0.8 + 0.89 = 1.69 m/s
Paddle Time: 10,000 / 1.69 / 3600 = 1.64 hours
```

---

## 7. Frontend

### 7.1 Technology Stack

| Layer | Technology | Version |
|-------|------------|---------|
| **Framework** | Astro | 4.x |
| **Map Library** | Mapbox GL JS | 3.3.0 |
| **Styling** | CSS Variables (custom) | — |
| **Routing (prototype)** | Client-side Dijkstra | — |
| **Charts** | Canvas API (custom) | — |

### 7.2 Key Components

#### Map Container

```html
<div id="map"></div>
```

Mapbox GL JS map with custom outdoor-fishing style, showing:
- Base map (terrain, land cover)
- River network (all edges as blue lines)
- Route highlight (yellow line)
- Put-in/Take-out markers (green/red)

#### Route Panel

```html
<div class="route-panel">
  <div class="panel-section"><!-- Route inputs --></div>
  <div id="route-stats"><!-- Trip statistics --></div>
  <div id="paddle-section"><!-- Paddle speed slider --></div>
  <div id="elevation-section"><!-- Elevation chart --></div>
</div>
```

#### Client-Side Router (Prototype)

The prototype uses client-side Dijkstra's algorithm with a pre-exported JSON graph:

```javascript
class RiverNetwork {
    constructor(data) {
        this.nodes = data.nodes;  // {nodeId: [lng, lat]}
        this.edges = data.edges;  // [{f, t, l, v, n, es, ee, c}]
        this.adj = {};            // Adjacency list
        
        // Build bidirectional adjacency
        data.edges.forEach((e, i) => {
            if (!this.adj[e.f]) this.adj[e.f] = [];
            this.adj[e.f].push({ node: e.t, idx: i });
            
            if (!this.adj[e.t]) this.adj[e.t] = [];
            this.adj[e.t].push({ node: e.f, idx: i });
        });
    }
    
    route(startNode, endNode) {
        // Dijkstra's algorithm
        // Returns path nodes and edge indices
    }
    
    calcStats(edgeIndices, paddleSpeedMph) {
        // Calculate distance, time, elevation
    }
}
```

### 7.3 Graph Data Format (JSON)

Exported from `export_graph.py` for client-side use:

```json
{
  "nodes": {
    "123456": [-72.756, 44.337],
    "123457": [-72.754, 44.339]
  },
  "edges": [
    {
      "f": "123456",       // from node
      "t": "123457",       // to node
      "l": 523,            // length (meters)
      "v": 0.72,           // velocity (m/s)
      "n": "Winooski River", // name
      "es": 185.2,         // elevation start (m)
      "ee": 184.8,         // elevation end (m)
      "o": 5,              // stream order
      "c": [[-72.756, 44.337], [-72.755, 44.338], ...] // coordinates
    }
  ],
  "meta": {
    "bbox": [-72.85, 44.28, -72.55, 44.48],
    "total_nodes": 2847,
    "total_edges": 3156,
    "region": "Vermont prototype"
  }
}
```

### 7.4 Elevation Profile

Custom canvas-based elevation chart:

```javascript
function drawElevationProfile(canvas, elevations) {
    const ctx = canvas.getContext('2d');
    // Draw gradient fill
    // Draw line
    // Draw axis labels (elevation in ft, distance in mi)
}
```

### 7.5 Map Layers

| Layer ID | Type | Source | Purpose |
|----------|------|--------|---------|
| `all-rivers` | line | GeoJSON | Background river network |
| `all-rivers-labels` | symbol | GeoJSON | Stream names |
| `route-glow` | line | GeoJSON | Route highlight (glow effect) |
| `route-line` | line | GeoJSON | Route highlight (main line) |

---

## 8. Deployment

### 8.1 Current Setup

| Component | Platform | Details |
|-----------|----------|---------|
| **API Server** | AWS EC2 (t3.large) | Ubuntu 24.04, 8GB RAM |
| **Database** | AWS RDS | PostgreSQL 16 + PostGIS, db.t3.small |
| **Frontend** | Vercel | Astro static site (Portfolio-Site) |
| **Redis** | Not deployed | Planned for NWM cache |

### 8.2 Infrastructure as Code (Terraform)

Located in `terraform/`:

```hcl
# Main resources provisioned
resource "aws_db_instance" "main" {
  identifier     = "river-router-db"
  engine         = "postgres"
  engine_version = "16.4"
  instance_class = var.rds_instance_class  # db.t3.small
  # ...
}

resource "aws_security_group" "rds" {
  # PostgreSQL access from allowed CIDRs
}
```

### 8.3 Docker Deployment

```yaml
# docker-compose.yml
services:
  api:
    build: .
    ports: ["8000:8000"]
    environment:
      - DATABASE_URL=postgresql://...
      - REDIS_URL=redis://redis:6379/0
    depends_on: [db, redis]
    
  db:
    image: postgis/postgis:16-3.4
    volumes: [postgres_data:/var/lib/postgresql/data]
    
  redis:
    image: redis:7-alpine
    
  nwm-ingest:
    build: .
    command: "while true; do python scripts/ingest_nwm.py; sleep 3600; done"
```

### 8.4 Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `DATABASE_URL` | PostgreSQL connection string | `postgresql://user:pass@host:5432/db` |
| `REDIS_URL` | Redis connection string | `redis://localhost:6379/0` |
| `GRAPH_PATH` | Path to serialized graph | `/app/data/graph/national.pkl` |
| `NWM_BUCKET` | S3 bucket for NWM data | `noaa-nwm-pds` |
| `NWM_CACHE_TTL` | Cache TTL in seconds | `3600` |
| `CORS_ORIGINS` | Allowed origins (comma-separated) | `http://localhost:3000` |

### 8.5 Production Checklist

- [ ] Deploy API behind load balancer (ALB)
- [ ] Enable RDS Multi-AZ for high availability
- [ ] Set up Redis ElastiCache cluster
- [ ] Configure CloudWatch monitoring
- [ ] Set up automated NWM ingest cron job
- [ ] Enable HTTPS with ACM certificate
- [ ] Configure auto-scaling for API containers
- [ ] Set up CI/CD pipeline (GitHub Actions)

---

## 9. Future Enhancements

### 9.1 National Vector Tileset

**Goal:** Enable national-scale river visualization without loading full GeoJSON.

**Approach:**
1. Generate MBTiles from PostGIS using Tippecanoe
2. Upload to Mapbox as tileset
3. Use `mapbox://` source in frontend

```bash
# Generate tiles with stream order-based zoom filtering
tippecanoe -o rivers.mbtiles \
  --layer=rivers \
  --minimum-zoom=3 \
  --maximum-zoom=14 \
  --drop-densest-as-needed \
  --extend-zooms-if-still-dropping \
  rivers.geojson

# Upload to Mapbox
mapbox upload lman967.rivers rivers.mbtiles
```

**Zoom-Level Filtering:**

| Zoom | Min Stream Order | Approx Features |
|------|------------------|-----------------|
| 3-5 | 7+ | ~35K |
| 6-8 | 5+ | ~150K |
| 9-11 | 3+ | ~500K |
| 12+ | All | ~3M |

### 9.2 NWM Cron Automation

**Current:** Manual/Docker loop  
**Target:** AWS-native scheduled ingest

```yaml
# Option 1: ECS Scheduled Task
Resources:
  NWMIngestTask:
    Type: AWS::ECS::TaskDefinition
    # ...
    
  NWMIngestSchedule:
    Type: AWS::Events::Rule
    Properties:
      ScheduleExpression: "rate(1 hour)"
      Targets:
        - Arn: !GetAtt ECSCluster.Arn
          RoleArn: !GetAtt EventsRole.Arn
          EcsParameters:
            TaskDefinitionArn: !Ref NWMIngestTask
```

### 9.3 Server-Side Routing API

**Current:** Client-side Dijkstra with JSON graph  
**Target:** Server-side routing with in-memory graph

```python
# app/core/graph.py
from functools import lru_cache
import networkx as nx
import pickle

@lru_cache(maxsize=1)
def load_graph():
    """Load national graph into memory (one-time)."""
    with open(settings.graph_path, 'rb') as f:
        return pickle.load(f)

# On startup
@app.on_event("startup")
async def load_network():
    app.state.graph = load_graph()
```

### 9.4 Advanced Features Roadmap

| Feature | Priority | Complexity | Description |
|---------|----------|------------|-------------|
| **Portage Detection** | High | Medium | Identify dams, falls requiring carry |
| **Hazard Markers** | High | Low | Display rapids, strainers, low-head dams |
| **Lake Crossings** | Medium | Medium | Route across waterbodies with wind estimates |
| **Multi-Day Trips** | Medium | Medium | Campsite suggestions, day segments |
| **Flow Alerts** | Medium | Low | Warn if conditions too high/low |
| **Offline Mode** | Low | High | Downloadable region data |
| **Mobile App** | Low | High | React Native implementation |

### 9.5 Data Quality Improvements

1. **Canada Integration:** Add CanVec/NHN data for cross-border routes
2. **Velocity Calibration:** Compare NWM predictions to USGS gage observations
3. **User Feedback:** Allow reporting of inaccurate times/conditions
4. **Seasonal Adjustments:** Use monthly EROM instead of annual

### 9.6 Performance Targets

| Metric | Current | Target |
|--------|---------|--------|
| Route response time | ~2s (client) | <500ms (server) |
| Snap response time | ~500ms | <50ms |
| Graph load time | N/A | <10s on startup |
| NWM data freshness | Manual | <2 hours |
| Concurrent users | 1 | 100+ |

---

## Appendix A: API Response Schemas

### Pydantic Models

```python
# app/api/schemas.py

class Coordinate(BaseModel):
    lat: float = Field(..., ge=-90, le=90)
    lng: float = Field(..., ge=-180, le=180)

class RouteRequest(BaseModel):
    put_in: Coordinate
    take_out: Coordinate
    paddle_speed_mph: float = Field(default=0.0, ge=0, le=10)

class RouteStats(BaseModel):
    distance_mi: float
    distance_km: float
    float_time_hours: float
    paddle_time_hours: float
    elevation_drop_ft: float
    gradient_ft_per_mi: float
    avg_flow_mph: float
    waterways: List[str]
    conditions_as_of: Optional[str]

class RouteResponse(BaseModel):
    route: dict  # GeoJSON FeatureCollection
    stats: RouteStats
    elevation_profile: List[ElevationPoint]
```

---

## Appendix B: NWM Ingest Script

```python
# scripts/ingest_nwm.py (simplified)

def main():
    # 1. Find latest NWM file
    url, timestamp = get_latest_nwm_url()
    
    # 2. Download NetCDF
    nc_path = download_nwm(url)
    
    # 3. Parse with xarray
    ds = xr.open_dataset(nc_path)
    comids = ds['feature_id'].values
    velocities = ds['velocity'].values
    
    # 4. Load to PostgreSQL (TRUNCATE + INSERT)
    conn = psycopg2.connect(DATABASE_URL)
    cur.execute("TRUNCATE TABLE nwm_velocity")
    execute_values(cur, """
        INSERT INTO nwm_velocity (comid, velocity_ms, streamflow_cms)
        VALUES %s
    """, data)
    
    # 5. Update timestamp
    cur.execute("UPDATE nwm_velocity SET updated_at = %s", (timestamp,))
```

---

## Appendix C: References

1. **NHDPlus V2 Documentation:** https://www.epa.gov/waterdata/nhdplus-national-hydrography-dataset-plus
2. **National Water Model:** https://water.noaa.gov/about/nwm
3. **pynhd Library:** https://github.com/hyriver/pynhd
4. **Mapbox GL JS:** https://docs.mapbox.com/mapbox-gl-js/
5. **NetworkX:** https://networkx.org/documentation/stable/
6. **PostGIS:** https://postgis.net/documentation/

---

*Document generated by Clawdbot for the River Router project.*
