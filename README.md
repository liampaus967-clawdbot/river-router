# 🛶 River Router - Data Ingestion

**Data pipeline scripts for Paddleways river routing system.**

This repo handles all data ingestion and preprocessing. The routing API lives in [nhdplus-explorer](https://github.com/liampaus967-clawdbot/nhdplus-explorer).

---

## Scripts

| Script | Purpose | Frequency |
|--------|---------|-----------|
| `usgs_gauges.py` | Fetch USGS gauge metadata & live readings | Hourly (cron) |
| `usgs_percentiles.py` | Generate DOY flow percentiles | Monthly/Manual |
| `nwm_realtime_ingest.py` | Fetch NWM velocity data | Hourly (cron) |

---

## Quick Start

```bash
# Setup
cd river-router
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Populate USGS gauges (one-time)
python app/data/usgs_gauges.py populate

# Fetch live readings (run hourly)
python app/data/usgs_gauges.py fetch

# Generate percentiles (run monthly)
python app/data/usgs_percentiles.py --all

# Fetch NWM velocities (run hourly)
python app/data/nwm_realtime_ingest.py
```

---

## Database Tables

| Table | Rows | Purpose |
|-------|------|---------|
| `river_edges` | 2.96M | NHDPlus flowlines (national) |
| `usgs_statistics` | 26.1M | DOY percentiles for gauges |
| `nwm_velocity` | 2.45M | Real-time NWM velocities |
| `usgs_gauges` | 11K | Gauge metadata |
| `usgs_readings` | 42K | Recent gauge readings |
| `hazards_dams` | 91K | Dam locations |

---

## Cron Setup

```bash
# /etc/cron.d/river-router

# USGS live readings - every hour at :15
15 * * * * ubuntu cd /home/ubuntu/clawd/river-router && ./venv/bin/python app/data/usgs_gauges.py fetch >> /var/log/usgs_readings.log 2>&1

# NWM velocities - every hour at :30
30 * * * * ubuntu cd /home/ubuntu/clawd/river-router && ./venv/bin/python app/data/nwm_realtime_ingest.py >> /var/log/nwm_ingest.log 2>&1
```

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│              nhdplus-explorer (Next.js)                     │
│  Frontend + Routing API (TypeScript)                        │
└────────────────────────┬────────────────────────────────────┘
                         │ SQL Queries
┌────────────────────────▼────────────────────────────────────┐
│                    PostgreSQL + PostGIS                     │
│  river_edges · nwm_velocity · usgs_* tables                │
└────────────────────────▲────────────────────────────────────┘
                         │ Data Ingestion
┌────────────────────────┴────────────────────────────────────┐
│              river-router (This Repo)                       │
│  Python scripts for USGS, NWM, percentiles                 │
└─────────────────────────────────────────────────────────────┘
```

---

## Data Sources

| Source | Description | Update Frequency |
|--------|-------------|------------------|
| **USGS NWIS** | Gauge locations & live readings | Real-time |
| **USGS hyswap** | Historical percentiles by DOY | Static (25yr baseline) |
| **NOAA NWM** | Real-time streamflow & velocity | Hourly |
| **NHDPlus V2** | River network geometry | Static |

---

## Configuration

Environment variables (`.env`):

```bash
DATABASE_URL=postgresql://user:pass@host:5432/river_router
```

---

## License

Proprietary — Paddleways / onWater
