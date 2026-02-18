# River Router Data Ingestion

Scripts for populating USGS gauge data and flow statistics.

## Overview

| Script | Purpose | Frequency |
|--------|---------|-----------|
| `usgs_gauges.py` | Fetch gauge metadata & current readings | Hourly (cron) |
| `usgs_percentiles.py` | Generate DOY percentile stats | Monthly/Manual |
| `usgs_live_conditions.py` | Live flow status, trends, temp | Hourly (cron) |
| `nwm_realtime_ingest.py` | Fetch NWM velocity data | Hourly (cron) |

---

## usgs_percentiles.py

Generates day-of-year flow percentiles for USGS gauges using historical daily values.
Based on the [FGP architecture](https://github.com/lpaus967/FGP).

### How It Works

1. **Fetch History**: Downloads 25 years of daily mean discharge via `dataretrieval.nwis.get_dv()`
2. **Calculate Percentiles**: Uses `hyswap` library to compute P5, P10, P25, P50, P75, P90, P95 for each day of year
3. **Store**: Saves to `usgs_statistics` table in PostgreSQL

### Usage

```bash
# Single state (~1 min per state)
python app/data/usgs_percentiles.py --state VT

# All 50 states (~2-3 hours)
python app/data/usgs_percentiles.py --all
```

### Output Table

```sql
usgs_statistics (
    site_no       VARCHAR(15),
    param_code    VARCHAR(5),    -- "00060" for discharge
    stat_type     VARCHAR(10),   -- p05, p10, p25, p50, p75, p90, p95
    month         SMALLINT,
    day           SMALLINT,
    value         FLOAT,         -- flow in CFS
    updated_at    TIMESTAMPTZ
)
```

### Lookup API

```python
from app.data.usgs_percentiles import get_current_percentile

# Compare current flow to historical percentiles
result = get_current_percentile(
    site_id="04282000",
    current_flow=500,  # CFS
    month=2,
    day=18
)

# Returns:
# {
#     "percentile": 42.3,
#     "flow_status": "Normal",           # Much Below/Below/Normal/Above/Much Above
#     "drought_status": None             # D0-D4 if in drought, else None
# }
```

### Flow Status Classification

| Percentile | Status | Drought Level |
|------------|--------|---------------|
| < 5 | Much Below Normal | D2 - Severe |
| 5-10 | Much Below Normal | D1 - Moderate |
| 10-25 | Below Normal | D0 - Abnormally Dry |
| 25-75 | Normal | — |
| 75-90 | Above Normal | — |
| > 90 | Much Above Normal | — |

### Dependencies

```
dataretrieval  # USGS data fetching
hyswap         # USGS percentile calculations
pandas
numpy
psycopg2
```

### Reference

Based on [FGP (Flow Gauge Percentiles)](https://github.com/lpaus967/FGP) architecture:
- **Pipeline A**: Batch generation of reference statistics (this script)
- **Pipeline B**: Live comparison of current flow to reference (usgs_live_conditions.py)

---

## usgs_live_conditions.py

Fetches live readings, compares to percentiles, detects trends.

### Features

- **Flow Status**: Compares current flow to DOY percentiles (Normal, Below Normal, etc.)
- **Drought Classification**: USDM methodology (D0-D4)
- **Flow Trend**: Rising/Falling/Stable based on 24h history
- **Temperature Trend**: Rising/Falling/Stable

### Usage

```bash
# All gauges (may be slow)
python app/data/usgs_live_conditions.py

# Single state
python app/data/usgs_live_conditions.py --state VT

# Specific sites
python app/data/usgs_live_conditions.py --sites 01010000,01010500
```

### Output Table

```sql
usgs_live_conditions (
    site_no         VARCHAR(15) PRIMARY KEY,
    timestamp       TIMESTAMPTZ,
    flow_cfs        DOUBLE PRECISION,
    gage_height_ft  DOUBLE PRECISION,
    water_temp_c    DOUBLE PRECISION,
    percentile      DOUBLE PRECISION,  -- 0-100
    flow_status     VARCHAR(50),       -- Normal, Below Normal, etc.
    drought_status  VARCHAR(50),       -- D0-D4 or NULL
    flow_trend      VARCHAR(20),       -- rising, falling, stable, unknown
    flow_trend_rate DOUBLE PRECISION,  -- % per hour
    temp_trend      VARCHAR(20),
    temp_trend_rate DOUBLE PRECISION,
    hours_since_peak DOUBLE PRECISION,
    updated_at      TIMESTAMPTZ
)
```

### Trend Detection

| Trend | Condition |
|-------|-----------|
| Rising | >10% total increase over 24h |
| Falling | >10% total decrease over 24h |
| Stable | Within ±10% |
| Unknown | <4 data points |

---

## Database Schema

See `/migrations/` for table definitions. Key tables:

- `usgs_gauges` - Gauge metadata (location, drainage area, etc.)
- `usgs_readings` - Current/recent instantaneous values
- `usgs_statistics` - Historical percentile thresholds by DOY
- `nwm_forecasts` - NWM short-range forecast data
