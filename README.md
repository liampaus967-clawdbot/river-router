# 🛶 River Router

**National river routing engine for Paddleways** — Select a put-in and take-out anywhere in the US and get a routed path with real-time float time estimates.

> Google Maps, but for water.

---

## Features

- **🗺️ Click-to-Route** — Select put-in and take-out on any US river
- **⏱️ Float Time Estimation** — Based on real hydrological velocity data
- **🌊 Real-Time Conditions** — Integrates NOAA National Water Model for current flow
- **📈 Elevation Profile** — See total drop and gradient along your route
- **🏋️ Paddle Speed Modifier** — Adjust estimates for your paddling effort
- **📊 Trip Stats** — Distance, time, waterway names, stream classification

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         FRONTEND                                │
│  Mapbox GL JS · Click-to-route · Elevation profile · Stats     │
└────────────────────────┬────────────────────────────────────────┘
                         │ REST API
┌────────────────────────▼────────────────────────────────────────┐
│                       ROUTING API                               │
│  FastAPI · A* routing · NWM integration · Sub-second response  │
└─────┬──────────────┬──────────────────┬─────────────────────────┘
      │              │                  │
┌─────▼──────┐ ┌─────▼──────┐  ┌───────▼────────┐
│  PostGIS   │ │  Graph     │  │  NWM Cache     │
│  NHDPlus   │ │  (in-mem)  │  │  (Redis)       │
│  V2 Data   │ │            │  │                │
└────────────┘ └────────────┘  └────────────────┘
```

---

## Data Sources

| Source | Description | Update Frequency |
|--------|-------------|------------------|
| **NHDPlus V2** | National river network (~2.7M reaches) | Static baseline |
| **National Water Model** | Real-time streamflow & velocity | Hourly |
| **EROM** | Mean monthly velocity estimates | Static (seasonal) |

---

## API Endpoints

### `POST /route`

Route between two points on the river network.

**Request:**
```json
{
  "put_in": { "lat": 44.337, "lng": -72.756 },
  "take_out": { "lat": 44.395, "lng": -72.614 },
  "paddle_speed_mph": 2.0
}
```

**Response:**
```json
{
  "route": { "type": "FeatureCollection", "features": [...] },
  "stats": {
    "distance_mi": 12.7,
    "float_time_hours": 4.2,
    "paddle_time_hours": 2.8,
    "elevation_drop_ft": 82,
    "gradient_ft_per_mi": 6.5,
    "avg_flow_mph": 1.1,
    "waterways": ["Winooski River"],
    "conditions_as_of": "2026-01-29T12:00:00Z"
  },
  "elevation_profile": [
    { "distance_m": 0, "elevation_m": 185.2 },
    { "distance_m": 500, "elevation_m": 184.8 },
    ...
  ]
}
```

### `GET /snap`

Snap a coordinate to the nearest river reach.

**Request:** `GET /snap?lat=44.337&lng=-72.756`

**Response:**
```json
{
  "comid": 4587234,
  "snap_point": { "lat": 44.3372, "lng": -72.7558 },
  "distance_m": 45,
  "reach_name": "Winooski River",
  "stream_order": 5
}
```

### `GET /reach/{comid}`

Get current conditions for a specific reach.

**Response:**
```json
{
  "comid": 4587234,
  "name": "Winooski River",
  "current_flow_cfs": 1240,
  "current_velocity_fps": 2.1,
  "source": "nwm",
  "as_of": "2026-01-29T12:00:00Z"
}
```

---

## Project Structure

```
river-router-api/
├── app/
│   ├── __init__.py
│   ├── main.py              # FastAPI app entry point
│   ├── api/
│   │   ├── routes.py        # API endpoints
│   │   └── schemas.py       # Pydantic models
│   ├── core/
│   │   ├── config.py        # Environment config
│   │   ├── graph.py         # River network graph
│   │   └── router.py        # A* routing algorithm
│   ├── data/
│   │   ├── nhdplus.py       # NHDPlus data loading
│   │   └── nwm.py           # National Water Model ingest
│   └── services/
│       ├── snap.py          # Point-to-reach snapping
│       └── stats.py         # Route statistics calculation
├── scripts/
│   ├── build_graph.py       # Build network graph from NHDPlus
│   ├── ingest_nwm.py        # NWM hourly ingest cron job
│   └── generate_tiles.py    # Generate vector tiles for frontend
├── data/                    # Local data files (gitignored)
│   ├── nhdplus/
│   ├── graph/
│   └── cache/
├── tests/
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── .env.example
└── README.md
```

---

## Development Setup

### Prerequisites

- Python 3.11+
- PostgreSQL 16 + PostGIS 3.4
- Redis (for NWM cache)
- ~50GB disk space for national data

### Quick Start

```bash
# Clone
git clone https://github.com/liampaus967-clawdbot/river-router.git
cd river-router

# Create virtual environment
python -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Copy environment config
cp .env.example .env
# Edit .env with your database credentials

# Build the network graph (one-time, ~30 min)
python scripts/build_graph.py

# Start the API
uvicorn app.main:app --reload
```

### Running with Docker

```bash
docker-compose up -d
```

---

## Data Preparation

### 1. Download NHDPlus V2

Download all 21 HUC2 regions from EPA:
```bash
# ~15-20 GB total
./scripts/download_nhdplus.sh
```

### 2. Build Network Graph

```bash
# Extracts topology, builds adjacency list, serializes to binary
python scripts/build_graph.py --output data/graph/national.pkl
```

### 3. Set Up NWM Ingest

Add to crontab for hourly updates:
```bash
0 * * * * /path/to/venv/bin/python /path/to/scripts/ingest_nwm.py
```

---

## Configuration

Environment variables (`.env`):

```bash
# Database
DATABASE_URL=postgresql://user:pass@localhost:5432/river_router
REDIS_URL=redis://localhost:6379/0

# Data paths
GRAPH_PATH=/data/graph/national.pkl
NHDPLUS_PATH=/data/nhdplus/

# NWM
NWM_BUCKET=noaa-nwm-pds
NWM_CACHE_TTL=3600

# API
API_HOST=0.0.0.0
API_PORT=8000
```

---

## Performance Targets

| Metric | Target |
|--------|--------|
| Route response time | < 500ms (95th percentile) |
| Snap response time | < 50ms |
| NWM data freshness | < 2 hours |
| Concurrent requests | 50+ |

---

## Roadmap

- [x] **Phase 0** — Prototype (Vermont subset, client-side)
- [ ] **Phase 1** — National static router with EROM velocities
- [ ] **Phase 2** — Real-time NWM integration
- [ ] **Phase 3** — Production hardening
- [ ] **Phase 4** — Advanced features (portages, hazards, lake crossings)

See [PROJECT_PLAN.md](../project-tracker/projects/river-router/PROJECT_PLAN.md) for detailed timeline.

---

## License

Proprietary — Paddleways / onWater

---

## Acknowledgments

- **USGS** — NHDPlus hydrological dataset
- **NOAA** — National Water Model
- **EPA** — NHDPlus distribution and maintenance
