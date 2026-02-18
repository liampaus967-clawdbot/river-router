#!/usr/bin/env python3
"""
USGS Live Conditions Monitor

Fetches live USGS readings, compares to historical percentiles,
and determines flow status + trends.

Based on FGP architecture (https://github.com/lpaus967/FGP)

Usage:
    python usgs_live_conditions.py                    # Update all gauges
    python usgs_live_conditions.py --state VT         # Single state
    python usgs_live_conditions.py --sites 01010000,01010500  # Specific sites
"""

import os
import sys
import logging
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, asdict
from typing import Optional, List, Dict, Any
import json

import numpy as np
import psycopg2
from psycopg2.extras import execute_values, RealDictCursor
import dataretrieval.nwis as nwis

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get('DATABASE_URL',
    'postgresql://river_router:Pacific1ride@river-router-db.c6xmmyu04pdo.us-east-1.rds.amazonaws.com:5432/river_router')

# Flow status thresholds (percentile-based)
FLOW_STATUS = {
    (0, 5): "Much Below Normal",
    (5, 10): "Below Normal", 
    (10, 25): "Below Normal",
    (25, 75): "Normal",
    (75, 90): "Above Normal",
    (90, 95): "Above Normal",
    (95, 100): "Much Above Normal",
}

# Drought classification (USDM methodology)
DROUGHT_THRESHOLDS = {
    2: "D4 - Exceptional Drought",
    5: "D3 - Extreme Drought",
    10: "D2 - Severe Drought",
    20: "D1 - Moderate Drought",
    30: "D0 - Abnormally Dry",
}

# Trend detection config
TREND_WINDOW_HOURS = 24
TREND_MIN_POINTS = 4
TREND_RISING_THRESHOLD = 10.0   # % change to classify as rising
TREND_FALLING_THRESHOLD = -10.0  # % change to classify as falling


@dataclass
class TrendResult:
    """Trend analysis result."""
    trend: str  # "rising" | "falling" | "stable" | "unknown"
    trend_rate: float  # % change per hour
    hours_since_peak: Optional[float]
    data_points: int


@dataclass
class LiveCondition:
    """Live conditions for a single gauge."""
    site_no: str
    timestamp: datetime
    
    # Current readings
    flow_cfs: Optional[float]
    gage_height_ft: Optional[float]
    water_temp_c: Optional[float]
    
    # Flow status (compared to percentiles)
    percentile: Optional[float]
    flow_status: Optional[str]
    drought_status: Optional[str]
    
    # Trends
    flow_trend: Optional[str]
    flow_trend_rate: Optional[float]
    temp_trend: Optional[str]
    temp_trend_rate: Optional[float]
    hours_since_peak: Optional[float]


def get_db_connection():
    return psycopg2.connect(DATABASE_URL)


def setup_conditions_table():
    """Create the live conditions table if it doesn't exist."""
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS usgs_live_conditions (
            site_no VARCHAR(15) PRIMARY KEY,
            timestamp TIMESTAMPTZ,
            
            -- Current readings
            flow_cfs DOUBLE PRECISION,
            gage_height_ft DOUBLE PRECISION,
            water_temp_c DOUBLE PRECISION,
            
            -- Flow status
            percentile DOUBLE PRECISION,
            flow_status VARCHAR(50),
            drought_status VARCHAR(50),
            
            -- Trends
            flow_trend VARCHAR(20),
            flow_trend_rate DOUBLE PRECISION,
            temp_trend VARCHAR(20),
            temp_trend_rate DOUBLE PRECISION,
            hours_since_peak DOUBLE PRECISION,
            
            -- Metadata
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );
        
        CREATE INDEX IF NOT EXISTS idx_live_conditions_status 
        ON usgs_live_conditions (flow_status);
        
        CREATE INDEX IF NOT EXISTS idx_live_conditions_trend
        ON usgs_live_conditions (flow_trend);
    """)
    
    conn.commit()
    cur.close()
    conn.close()
    logger.info("✅ usgs_live_conditions table ready")


def fetch_live_readings(site_nos: List[str] = None, state: str = None) -> Dict[str, Dict]:
    """
    Fetch current instantaneous values from USGS.
    
    Returns dict: site_no -> {flow_cfs, gage_height_ft, water_temp_c, timestamp}
    """
    logger.info(f"📡 Fetching live readings...")
    
    try:
        if state:
            # Fetch by state
            df, _ = nwis.get_iv(stateCd=state, parameterCd="00060,00065,00010")
        elif site_nos:
            # Fetch specific sites (batch in groups of 100)
            all_dfs = []
            for i in range(0, len(site_nos), 100):
                batch = site_nos[i:i+100]
                df, _ = nwis.get_iv(sites=batch, parameterCd="00060,00065,00010")
                if not df.empty:
                    all_dfs.append(df)
            df = pd.concat(all_dfs) if all_dfs else pd.DataFrame()
        else:
            # Fetch all from our database
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("SELECT site_no FROM usgs_gauges LIMIT 1000")
            sites = [row[0] for row in cur.fetchall()]
            cur.close()
            conn.close()
            return fetch_live_readings(site_nos=sites)
        
        if df.empty:
            return {}
        
        # Parse results
        readings = {}
        
        # Group by site
        df = df.reset_index()
        
        for site_no in df['site_no'].unique():
            site_data = df[df['site_no'] == site_no].iloc[-1]  # Latest reading
            
            # Find columns (they vary: 00060, 00060_Mean, etc.)
            flow = None
            gage = None
            temp = None
            
            for col in df.columns:
                if '00060' in str(col) and 'cd' not in str(col).lower():
                    val = site_data.get(col)
                    if val is not None and not np.isnan(val) and val > 0:  # Filter -999999
                        flow = float(val)
                elif '00065' in str(col) and 'cd' not in str(col).lower():
                    val = site_data.get(col)
                    if val is not None and not np.isnan(val) and val > -100:  # Filter invalid
                        gage = float(val)
                elif '00010' in str(col) and 'cd' not in str(col).lower():
                    val = site_data.get(col)
                    if val is not None and not np.isnan(val) and val > -50:  # Filter invalid
                        temp = float(val)
            
            readings[site_no] = {
                'flow_cfs': flow,
                'gage_height_ft': gage,
                'water_temp_c': temp,
                'timestamp': datetime.now(timezone.utc)
            }
        
        logger.info(f"   Got readings for {len(readings)} sites")
        return readings
        
    except Exception as e:
        logger.error(f"Error fetching live readings: {e}")
        return {}


def get_historical_readings(site_no: str, hours: int = 24) -> List[Dict]:
    """Get historical readings from usgs_readings table for trend detection."""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    cur.execute("""
        SELECT reading_time, streamflow_cfs, water_temp_c
        FROM usgs_readings
        WHERE site_no = %s 
        AND reading_time > NOW() - INTERVAL '%s hours'
        ORDER BY reading_time ASC
    """, (site_no, hours))
    
    rows = cur.fetchall()
    cur.close()
    conn.close()
    
    return [dict(r) for r in rows]


def calculate_trend(values: List[tuple], threshold_rising: float = 10.0, 
                   threshold_falling: float = -10.0) -> TrendResult:
    """
    Calculate trend from time series data.
    
    Args:
        values: List of (timestamp, value) tuples
        threshold_rising: % total change to classify as rising
        threshold_falling: % total change to classify as falling
    
    Returns:
        TrendResult with trend classification
    """
    if len(values) < TREND_MIN_POINTS:
        return TrendResult("unknown", 0.0, None, len(values))
    
    # Extract arrays
    timestamps = [v[0] for v in values]
    vals = np.array([v[1] for v in values])
    
    # Check for constant values
    if np.std(vals) < 1e-10:
        return TrendResult("stable", 0.0, None, len(values))
    
    # Convert to hours from start
    base_time = timestamps[0]
    hours = np.array([(t - base_time).total_seconds() / 3600.0 for t in timestamps])
    
    total_hours = hours[-1] - hours[0]
    if total_hours < 0.5:  # Less than 30 min of data
        return TrendResult("unknown", 0.0, None, len(values))
    
    # Normalize: (val - median) / median * 100
    median_val = np.median(vals)
    if median_val < 1e-10:
        return TrendResult("unknown", 0.0, None, len(values))
    
    normalized = (vals - median_val) / median_val * 100
    
    # Linear regression
    slope, _ = np.polyfit(hours, normalized, 1)
    trend_rate = float(slope)
    
    # Total change over window
    total_change = trend_rate * total_hours
    
    # Find peak for hours_since_peak
    peak_idx = np.argmax(vals)
    peak_time = timestamps[peak_idx]
    now = timestamps[-1]
    hours_since_peak = (now - peak_time).total_seconds() / 3600.0
    
    # Classify
    if total_change >= threshold_rising:
        return TrendResult("rising", round(trend_rate, 2), None, len(values))
    elif total_change <= threshold_falling:
        hsp = round(hours_since_peak, 1) if hours_since_peak > 0.5 else None
        return TrendResult("falling", round(trend_rate, 2), hsp, len(values))
    else:
        return TrendResult("stable", round(trend_rate, 2), None, len(values))


def get_percentile_thresholds(site_no: str, month: int, day: int) -> Dict[int, float]:
    """Get percentile thresholds from usgs_statistics table."""
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT stat_type, value
        FROM usgs_statistics
        WHERE site_no = %s AND month = %s AND day = %s
        AND stat_type LIKE 'p%%'
    """, (site_no, month, day))
    
    thresholds = {}
    for stat_type, value in cur.fetchall():
        try:
            # Parse p05, p10, etc.
            pct = int(stat_type[1:])
            thresholds[pct] = float(value)
        except (ValueError, IndexError):
            pass
    
    cur.close()
    conn.close()
    return thresholds


def interpolate_percentile(current_flow: float, thresholds: Dict[int, float]) -> Optional[float]:
    """Interpolate exact percentile from thresholds."""
    if not thresholds or len(thresholds) < 2:
        return None
    
    sorted_pcts = sorted(thresholds.keys())
    sorted_vals = [thresholds[p] for p in sorted_pcts]
    
    if current_flow <= sorted_vals[0]:
        return float(sorted_pcts[0])
    if current_flow >= sorted_vals[-1]:
        return float(sorted_pcts[-1])
    
    return float(np.interp(current_flow, sorted_vals, sorted_pcts))


def get_flow_status(percentile: float) -> str:
    """Get flow status label from percentile."""
    for (low, high), status in FLOW_STATUS.items():
        if low <= percentile < high:
            return status
    return "Normal"


def get_drought_status(percentile: float) -> Optional[str]:
    """Get drought classification if applicable."""
    for threshold, status in sorted(DROUGHT_THRESHOLDS.items()):
        if percentile < threshold:
            return status
    return None


def compute_live_condition(site_no: str, reading: Dict) -> LiveCondition:
    """Compute full live condition for a site."""
    now = datetime.now(timezone.utc)
    
    # Get percentile thresholds for today
    thresholds = get_percentile_thresholds(site_no, now.month, now.day)
    
    # Calculate percentile and status
    percentile = None
    flow_status = None
    drought_status = None
    
    if reading['flow_cfs'] is not None and thresholds:
        percentile = interpolate_percentile(reading['flow_cfs'], thresholds)
        if percentile is not None:
            flow_status = get_flow_status(percentile)
            drought_status = get_drought_status(percentile)
    
    # Get historical data for trends
    history = get_historical_readings(site_no, hours=TREND_WINDOW_HOURS)
    
    # Calculate flow trend
    flow_trend_result = TrendResult("unknown", 0.0, None, 0)
    if history and reading['flow_cfs'] is not None:
        flow_values = [(h['reading_time'], h['streamflow_cfs']) 
                       for h in history if h['streamflow_cfs'] is not None]
        # Add current reading
        flow_values.append((now, reading['flow_cfs']))
        if len(flow_values) >= TREND_MIN_POINTS:
            flow_trend_result = calculate_trend(flow_values, 
                TREND_RISING_THRESHOLD, TREND_FALLING_THRESHOLD)
    
    # Calculate temp trend
    temp_trend = None
    temp_trend_rate = None
    if history and reading['water_temp_c'] is not None:
        temp_values = [(h['reading_time'], h['water_temp_c']) 
                       for h in history if h['water_temp_c'] is not None]
        temp_values.append((now, reading['water_temp_c']))
        if len(temp_values) >= TREND_MIN_POINTS:
            temp_result = calculate_trend(temp_values, 5.0, -5.0)  # Smaller thresholds for temp
            temp_trend = temp_result.trend
            temp_trend_rate = temp_result.trend_rate
    
    return LiveCondition(
        site_no=site_no,
        timestamp=reading['timestamp'],
        flow_cfs=reading['flow_cfs'],
        gage_height_ft=reading['gage_height_ft'],
        water_temp_c=reading['water_temp_c'],
        percentile=round(percentile, 1) if percentile else None,
        flow_status=flow_status,
        drought_status=drought_status,
        flow_trend=flow_trend_result.trend,
        flow_trend_rate=flow_trend_result.trend_rate,
        temp_trend=temp_trend,
        temp_trend_rate=temp_trend_rate,
        hours_since_peak=flow_trend_result.hours_since_peak
    )


def save_conditions(conditions: List[LiveCondition]):
    """Save live conditions to database."""
    if not conditions:
        return
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    values = [
        (c.site_no, c.timestamp, c.flow_cfs, c.gage_height_ft, c.water_temp_c,
         c.percentile, c.flow_status, c.drought_status,
         c.flow_trend, c.flow_trend_rate, c.temp_trend, c.temp_trend_rate,
         c.hours_since_peak, datetime.now(timezone.utc))
        for c in conditions
    ]
    
    execute_values(cur, """
        INSERT INTO usgs_live_conditions 
        (site_no, timestamp, flow_cfs, gage_height_ft, water_temp_c,
         percentile, flow_status, drought_status,
         flow_trend, flow_trend_rate, temp_trend, temp_trend_rate,
         hours_since_peak, updated_at)
        VALUES %s
        ON CONFLICT (site_no) DO UPDATE SET
            timestamp = EXCLUDED.timestamp,
            flow_cfs = EXCLUDED.flow_cfs,
            gage_height_ft = EXCLUDED.gage_height_ft,
            water_temp_c = EXCLUDED.water_temp_c,
            percentile = EXCLUDED.percentile,
            flow_status = EXCLUDED.flow_status,
            drought_status = EXCLUDED.drought_status,
            flow_trend = EXCLUDED.flow_trend,
            flow_trend_rate = EXCLUDED.flow_trend_rate,
            temp_trend = EXCLUDED.temp_trend,
            temp_trend_rate = EXCLUDED.temp_trend_rate,
            hours_since_peak = EXCLUDED.hours_since_peak,
            updated_at = EXCLUDED.updated_at
    """, values)
    
    conn.commit()
    cur.close()
    conn.close()


def run_live_monitor(state: str = None, sites: List[str] = None):
    """Run the live monitoring pipeline."""
    print("=" * 60)
    print(f"📊 USGS Live Conditions Monitor")
    print(f"   Started: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 60)
    
    # Ensure table exists
    setup_conditions_table()
    
    # Fetch live readings
    readings = fetch_live_readings(site_nos=sites, state=state)
    
    if not readings:
        print("⚠️  No readings fetched")
        return
    
    print(f"\n🔄 Processing {len(readings)} sites...")
    
    # Compute conditions for each site
    conditions = []
    stats = {"with_percentile": 0, "rising": 0, "falling": 0, "stable": 0}
    
    for i, (site_no, reading) in enumerate(readings.items()):
        try:
            condition = compute_live_condition(site_no, reading)
            conditions.append(condition)
            
            if condition.percentile:
                stats["with_percentile"] += 1
            if condition.flow_trend == "rising":
                stats["rising"] += 1
            elif condition.flow_trend == "falling":
                stats["falling"] += 1
            elif condition.flow_trend == "stable":
                stats["stable"] += 1
                
        except Exception as e:
            logger.debug(f"Error processing {site_no}: {e}")
        
        if (i + 1) % 100 == 0:
            print(f"   Processed {i + 1}/{len(readings)} sites...")
    
    # Save to database
    print(f"\n💾 Saving {len(conditions)} conditions...")
    save_conditions(conditions)
    
    print(f"\n" + "=" * 60)
    print(f"✅ Complete!")
    print(f"   Sites processed: {len(conditions)}")
    print(f"   With percentile: {stats['with_percentile']}")
    print(f"   Rising: {stats['rising']} | Falling: {stats['falling']} | Stable: {stats['stable']}")
    print("=" * 60)


def main():
    import argparse
    import pandas as pd  # Need this for fetch_live_readings
    
    parser = argparse.ArgumentParser(description="USGS Live Conditions Monitor")
    parser.add_argument('--state', type=str, help='State code (e.g., VT)')
    parser.add_argument('--sites', type=str, help='Comma-separated site numbers')
    args = parser.parse_args()
    
    sites = args.sites.split(',') if args.sites else None
    run_live_monitor(state=args.state, sites=sites)


if __name__ == "__main__":
    # Import pandas here to avoid startup delay
    import pandas as pd
    main()
