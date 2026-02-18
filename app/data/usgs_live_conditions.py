#!/usr/bin/env python3
"""
USGS Live Conditions Monitor

Fetches live USGS readings, compares to historical percentiles (from S3),
determines flow status + trends, and uploads results to S3.

Usage:
    python usgs_live_conditions.py                    # All states with reference data
    python usgs_live_conditions.py --state VT         # Single state
    python usgs_live_conditions.py --bucket my-bucket # Custom bucket
"""

import os
import logging
from datetime import datetime, timezone
from dataclasses import dataclass, asdict
from typing import Optional, List, Dict

import numpy as np
import pandas as pd
import dataretrieval.nwis as nwis

from app.data.s3_client import S3Client

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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
TREND_RISING_THRESHOLD = 10.0
TREND_FALLING_THRESHOLD = -10.0


@dataclass
class TrendResult:
    trend: str  # "rising" | "falling" | "stable" | "unknown"
    trend_rate: float
    hours_since_peak: Optional[float]
    data_points: int


def fetch_state_readings(state_code: str) -> Dict[str, Dict]:
    """Fetch current instantaneous values for a state."""
    try:
        df, _ = nwis.get_iv(
            stateCd=state_code,
            parameterCd="00060,00065,00010"  # Flow, gage height, temp
        )
        
        if df.empty:
            return {}
        
        readings = {}
        df = df.reset_index()
        
        for site_no in df['site_no'].unique():
            site_data = df[df['site_no'] == site_no].iloc[-1]
            
            flow = None
            gage = None
            temp = None
            
            for col in df.columns:
                if '00060' in str(col) and 'cd' not in str(col).lower():
                    val = site_data.get(col)
                    if val is not None and not np.isnan(val) and val > 0:
                        flow = float(val)
                elif '00065' in str(col) and 'cd' not in str(col).lower():
                    val = site_data.get(col)
                    if val is not None and not np.isnan(val) and val > -100:
                        gage = float(val)
                elif '00010' in str(col) and 'cd' not in str(col).lower():
                    val = site_data.get(col)
                    if val is not None and not np.isnan(val) and val > -50:
                        temp = float(val)
            
            readings[site_no] = {
                'flow': flow,
                'gage_height': gage,
                'water_temp': temp,
            }
        
        return readings
        
    except Exception as e:
        logger.error(f"Error fetching readings for {state_code}: {e}")
        return {}


def calculate_trend(values: List[tuple]) -> TrendResult:
    """Calculate trend from time series data."""
    if len(values) < TREND_MIN_POINTS:
        return TrendResult("unknown", 0.0, None, len(values))
    
    timestamps = [v[0] for v in values]
    vals = np.array([v[1] for v in values])
    
    if np.std(vals) < 1e-10:
        return TrendResult("stable", 0.0, None, len(values))
    
    base_time = timestamps[0]
    hours = np.array([(t - base_time).total_seconds() / 3600.0 for t in timestamps])
    
    total_hours = hours[-1] - hours[0]
    if total_hours < 0.5:
        return TrendResult("unknown", 0.0, None, len(values))
    
    median_val = np.median(vals)
    if median_val < 1e-10:
        return TrendResult("unknown", 0.0, None, len(values))
    
    normalized = (vals - median_val) / median_val * 100
    slope, _ = np.polyfit(hours, normalized, 1)
    trend_rate = float(slope)
    total_change = trend_rate * total_hours
    
    peak_idx = np.argmax(vals)
    peak_time = timestamps[peak_idx]
    now = timestamps[-1]
    hours_since_peak = (now - peak_time).total_seconds() / 3600.0
    
    if total_change >= TREND_RISING_THRESHOLD:
        return TrendResult("rising", round(trend_rate, 2), None, len(values))
    elif total_change <= TREND_FALLING_THRESHOLD:
        hsp = round(hours_since_peak, 1) if hours_since_peak > 0.5 else None
        return TrendResult("falling", round(trend_rate, 2), hsp, len(values))
    else:
        return TrendResult("stable", round(trend_rate, 2), None, len(values))


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
    for (low, high), status in FLOW_STATUS.items():
        if low <= percentile < high:
            return status
    return "Normal"


def get_drought_status(percentile: float) -> Optional[str]:
    for threshold, status in sorted(DROUGHT_THRESHOLDS.items()):
        if percentile < threshold:
            return status
    return None


def process_state(state_code: str, s3_client: S3Client, all_history: Dict) -> Dict[str, Dict]:
    """Process all sites in a state and return conditions dict."""
    
    # Load reference stats from S3
    ref_df = s3_client.download_reference_stats(state_code)
    if ref_df is None:
        logger.warning(f"No reference stats for {state_code}")
        return {}
    
    # Get today's month-day
    now = datetime.now(timezone.utc)
    month_day = now.strftime("%m-%d")
    
    # Filter reference to today
    today_ref = ref_df[ref_df['month_day'] == month_day]
    
    # Build lookup: site_id -> {p05: val, p10: val, ...}
    ref_lookup = {}
    for _, row in today_ref.iterrows():
        site_id = row['site_id']
        thresholds = {}
        for col in row.index:
            if col.startswith('p') and col[1:].isdigit():
                pct = int(col[1:])
                if pd.notna(row[col]):
                    thresholds[pct] = float(row[col])
        if thresholds:
            ref_lookup[site_id] = thresholds
    
    # Fetch live readings
    readings = fetch_state_readings(state_code)
    if not readings:
        return {}
    
    # Process each site
    conditions = {}
    
    for site_no, reading in readings.items():
        site_data = {
            "flow": reading['flow'],
            "gage_height": reading['gage_height'],
            "water_temp": reading['water_temp'],
            "percentile": None,
            "flow_status": None,
            "drought_status": None,
            "trend": None,
            "trend_rate": None,
            "hours_since_peak": None,
            "state": state_code,
        }
        
        # Calculate percentile if we have reference data
        if reading['flow'] is not None and site_no in ref_lookup:
            percentile = interpolate_percentile(reading['flow'], ref_lookup[site_no])
            if percentile is not None:
                site_data["percentile"] = round(percentile, 1)
                site_data["flow_status"] = get_flow_status(percentile)
                site_data["drought_status"] = get_drought_status(percentile)
        
        # Calculate trend from historical snapshots
        if site_no in all_history and reading['flow'] is not None:
            history = all_history[site_no]
            # Add current reading
            history.append((now, reading['flow']))
            
            if len(history) >= TREND_MIN_POINTS:
                trend_result = calculate_trend(history)
                site_data["trend"] = trend_result.trend
                site_data["trend_rate"] = trend_result.trend_rate
                site_data["hours_since_peak"] = trend_result.hours_since_peak
        
        conditions[site_no] = site_data
    
    return conditions


def run_live_monitor(states: List[str] = None, bucket: str = None):
    """Run the live monitoring pipeline."""
    print("=" * 60)
    print(f"📊 USGS Live Conditions Monitor")
    print(f"   Started: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 60)
    
    s3_client = S3Client(bucket_name=bucket)
    
    # Get available states if not specified
    if states is None:
        states = s3_client.list_available_states()
        if not states:
            print("⚠️  No reference data found in S3. Run usgs_percentiles.py first.")
            return
    
    print(f"\n🔍 Processing {len(states)} states...")
    
    # Load historical snapshots for trend detection
    print("📥 Loading historical snapshots for trends...")
    historical_keys = s3_client.list_historical_snapshots(hours=TREND_WINDOW_HOURS)
    
    # Build site history from snapshots
    all_history: Dict[str, List] = {}
    for key in historical_keys:
        snapshot = s3_client.download_historical_snapshot(key)
        if not snapshot:
            continue
        
        # Parse timestamp from key
        filename = key.split("/")[-1]
        try:
            timestamp_str = filename.replace(".json", "")
            timestamp = datetime.strptime(timestamp_str, "%Y-%m-%dT%H%M")
        except ValueError:
            continue
        
        sites = snapshot.get("sites", {})
        for site_id, site_data in sites.items():
            flow = site_data.get("flow")
            if flow is not None:
                if site_id not in all_history:
                    all_history[site_id] = []
                all_history[site_id].append((timestamp, flow))
    
    print(f"   Loaded history for {len(all_history)} sites")
    
    # Process each state
    all_conditions = {}
    stats = {"with_percentile": 0, "rising": 0, "falling": 0, "stable": 0}
    
    for state in states:
        logger.info(f"Processing {state}...")
        conditions = process_state(state, s3_client, all_history)
        
        for site_no, data in conditions.items():
            all_conditions[site_no] = data
            
            if data.get("percentile"):
                stats["with_percentile"] += 1
            if data.get("trend") == "rising":
                stats["rising"] += 1
            elif data.get("trend") == "falling":
                stats["falling"] += 1
            elif data.get("trend") == "stable":
                stats["stable"] += 1
    
    # Upload to S3
    print(f"\n💾 Uploading {len(all_conditions)} sites to S3...")
    
    output = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "site_count": len(all_conditions),
        "sites": all_conditions
    }
    
    s3_client.upload_live_output(output)
    
    print(f"\n" + "=" * 60)
    print(f"✅ Complete!")
    print(f"   Sites: {len(all_conditions)}")
    print(f"   With percentile: {stats['with_percentile']}")
    print(f"   Rising: {stats['rising']} | Falling: {stats['falling']} | Stable: {stats['stable']}")
    print("=" * 60)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="USGS Live Conditions Monitor")
    parser.add_argument('--state', type=str, help='State code (e.g., VT)')
    parser.add_argument('--bucket', type=str, help='S3 bucket name')
    args = parser.parse_args()
    
    states = [args.state.upper()] if args.state else None
    run_live_monitor(states=states, bucket=args.bucket)


if __name__ == "__main__":
    main()
