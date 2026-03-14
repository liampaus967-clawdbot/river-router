# Flow Percentiles System

Historical flow analysis for Vermont river reaches using NWM retrospective data.

## Overview

Calculates flow/velocity percentiles by comparing current NWM data against 10 years of historical averages. Uses weekly windows for smoother curves.

## Tables

- `flow_history` - Raw daily averages per comid (51M rows for 10yr VT data)
- `flow_percentiles` - Pre-computed percentile breakpoints per comid per week (~730K rows)

## Data Source

NWM Retrospective v3.0 Zarr Store:
```
s3://noaa-nwm-retrospective-3-0-pds/CONUS/zarr/chrtout.zarr/
```
- 385K hourly timesteps (1979-2023, 44+ years!)
- 2.7M feature_ids (all CONUS reaches)
- Streams directly from S3 via Zarr (no download required)

## Scripts

- `schema.sql` - Database table definitions ✅ (applied)
- `fetch_retrospective.py` - ETL script to load NWM data from S3 ✅
- `compute_percentiles.py` - Calculate percentile breakpoints (TODO)
- `update_current.py` - Compare current flow to percentiles (TODO)

## Usage

```bash
# 1. Create tables (already done)
psql -f schema.sql

# 2. Test run (100 comids, ~30 days)
python fetch_retrospective.py --test

# 3. Full Vermont load (14K comids, 10 years) 
python fetch_retrospective.py --state VT --years 10

# 4. Custom comids from file
python fetch_retrospective.py --comids my_comids.txt --years 5

# 5. Compute percentiles (after data is loaded)
python compute_percentiles.py
```

## Environment Variables

```bash
export DB_HOST=driftwise-west.cfs02ime4lxt.us-west-2.rds.amazonaws.com
export DB_NAME=gisdata
export DB_USER=postgres
export DB_PASSWORD=driftingInVermont
```

## Memory Considerations

The ETL processes data in chunks (default 30 days) to avoid OOM. For EC2 t2.micro (1GB RAM), use `--chunk-days 7`.
