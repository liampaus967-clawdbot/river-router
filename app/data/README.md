# River Router Data Ingestion

Data pipeline scripts for Paddleways river routing system.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        S3 Bucket                            │
│  reference_stats/state=XX/data.parquet  (percentiles)      │
│  live_output/current_status.json        (live conditions)  │
│  live_output/history/YYYY-MM-DDTHHMM.json (for trends)     │
└─────────────────────────────────────────────────────────────┘
                              ↑
                         Upload/Download
                              ↑
┌─────────────────────────────────────────────────────────────┐
│                    Python Scripts                           │
│  usgs_percentiles.py      → S3 (reference stats)           │
│  usgs_live_conditions.py  → S3 (live output)               │
│  usgs_gauges.py           → PostgreSQL (geometry only)     │
│  nwm_realtime_ingest.py   → PostgreSQL (velocities)        │
└─────────────────────────────────────────────────────────────┘
```

## Scripts

| Script | Output | Frequency |
|--------|--------|-----------|
| `usgs_percentiles.py` | S3 Parquet (by state) | Monthly |
| `usgs_live_conditions.py` | S3 JSON | Hourly |
| `usgs_gauges.py` | PostgreSQL | One-time |
| `nwm_realtime_ingest.py` | PostgreSQL | Hourly |

---

## Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Configure AWS credentials (or use IAM role)
export S3_BUCKET=river-router-data
export AWS_REGION=us-east-1

# Configure database
export DATABASE_URL=postgresql://user:pass@host:5432/river_router
```

---

## usgs_percentiles.py

Generates DOY-based flow percentiles from 25 years of historical data.
Uploads to S3 as Parquet, partitioned by state.

### Usage

```bash
# Single state (~1-2 min)
python app/data/usgs_percentiles.py --state VT

# All 50 states (~2-3 hours)
python app/data/usgs_percentiles.py --all

# Custom bucket
python app/data/usgs_percentiles.py --all --bucket my-bucket
```

### S3 Output

```
s3://bucket/reference_stats/
├── state=AL/data.parquet
├── state=AK/data.parquet
├── state=VT/data.parquet
└── ...
```

### Parquet Schema

| Column | Type | Description |
|--------|------|-------------|
| site_id | string | USGS site number |
| month_day | string | "MM-DD" format |
| p05 | float | 5th percentile flow (CFS) |
| p10 | float | 10th percentile |
| p25 | float | 25th percentile |
| p50 | float | 50th percentile (median) |
| p75 | float | 75th percentile |
| p90 | float | 90th percentile |
| p95 | float | 95th percentile |
| count | int | Years of data |

---

## usgs_live_conditions.py

Fetches live readings, compares to percentiles, detects trends.
Uploads to S3 as JSON.

### Usage

```bash
# All states with reference data
python app/data/usgs_live_conditions.py

# Single state
python app/data/usgs_live_conditions.py --state VT
```

### S3 Output

```
s3://bucket/live_output/
├── current_status.json          # Latest snapshot
└── history/
    ├── 2026-02-18T0100.json    # Hourly snapshots
    ├── 2026-02-18T0200.json    # (for trend detection)
    └── ...
```

### JSON Schema

```json
{
  "generated_at": "2026-02-18T01:30:00Z",
  "site_count": 10000,
  "sites": {
    "01010000": {
      "flow": 1234.5,
      "gage_height": 5.2,
      "water_temp": 8.5,
      "percentile": 45.2,
      "flow_status": "Normal",
      "drought_status": null,
      "trend": "rising",
      "trend_rate": 2.5,
      "hours_since_peak": null,
      "state": "ME"
    }
  }
}
```

### Flow Status Classification

| Percentile | Status | Drought Level |
|------------|--------|---------------|
| < 5 | Much Below Normal | D2+ |
| 5-25 | Below Normal | D0-D1 |
| 25-75 | Normal | — |
| 75-95 | Above Normal | — |
| > 95 | Much Above Normal | — |

### Trend Detection

| Trend | Condition |
|-------|-----------|
| Rising | >10% increase over 24h |
| Falling | >10% decrease over 24h |
| Stable | Within ±10% |
| Unknown | <4 historical snapshots |

---

## usgs_gauges.py

Populates gauge locations in PostgreSQL (geometry only).

```bash
# One-time population
python app/data/usgs_gauges.py populate

# Fetch live readings (legacy, prefer usgs_live_conditions.py)
python app/data/usgs_gauges.py fetch
```

---

## Cron Setup

```bash
# /etc/cron.d/river-router

# Generate percentiles (monthly)
0 3 1 * * ubuntu cd /path/to/river-router && ./venv/bin/python app/data/usgs_percentiles.py --all

# Update live conditions (hourly)
15 * * * * ubuntu cd /path/to/river-router && ./venv/bin/python app/data/usgs_live_conditions.py

# Update NWM velocities (hourly)
30 * * * * ubuntu cd /path/to/river-router && ./venv/bin/python app/data/nwm_realtime_ingest.py
```

---

## Dependencies

```
boto3           # AWS S3
pandas          # Data manipulation
pyarrow         # Parquet support
dataretrieval   # USGS API
hyswap          # USGS percentile calculations
numpy
psycopg2        # PostgreSQL (for gauges/NWM only)
```
