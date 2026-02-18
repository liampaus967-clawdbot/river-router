#!/usr/bin/env python3
"""
USGS Flow Percentile Generator

Fetches 25 years of historical daily values for each USGS gauge
and calculates DOY-based percentiles using hyswap.

Stores results in S3 as Parquet files, partitioned by state.

Usage:
    python usgs_percentiles.py --state VT     # Single state
    python usgs_percentiles.py --all          # All 50 states
"""

import os
import logging
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, List

import pandas as pd
import numpy as np
import dataretrieval.nwis as nwis

# Try hyswap (official USGS library)
try:
    import hyswap
    HYSWAP_AVAILABLE = True
except ImportError:
    HYSWAP_AVAILABLE = False
    logging.warning("hyswap not installed - using manual percentile calculation")

from app.data.s3_client import S3Client

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Config
PARAM_CODE = "00060"  # Discharge
START_DATE = "2000-01-01"  # ~25 years of data
PERCENTILES = (5, 10, 25, 50, 75, 90, 95)
MAX_WORKERS = 5

# All US states
ALL_STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"
]


def get_sites_for_state(state_code: str) -> List[str]:
    """Get all active streamflow sites for a state."""
    try:
        sites, _ = nwis.get_info(
            stateCd=state_code,
            parameterCd=PARAM_CODE,
            siteType="ST",
            siteStatus="active"
        )
        if sites.empty:
            return []
        return sites["site_no"].tolist()
    except Exception as e:
        logger.error(f"Error fetching sites for {state_code}: {e}")
        return []


def fetch_site_history(site_id: str) -> Optional[pd.DataFrame]:
    """Fetch 25 years of daily values for a site."""
    try:
        df, _ = nwis.get_dv(
            sites=site_id,
            parameterCd=PARAM_CODE,
            start=START_DATE
        )
        
        if df.empty:
            return None
        
        # Find discharge column
        discharge_cols = [c for c in df.columns if "00060" in c and "cd" not in c.lower()]
        if not discharge_cols:
            return None
        
        col = discharge_cols[0]
        # Filter invalid values
        df = df[df[col] > 0]
        
        return df if not df.empty else None
        
    except Exception as e:
        logger.debug(f"Error fetching history for {site_id}: {e}")
        return None


def calculate_percentiles_hyswap(df: pd.DataFrame, site_id: str) -> Optional[pd.DataFrame]:
    """Calculate percentiles using hyswap library."""
    if not HYSWAP_AVAILABLE:
        return calculate_percentiles_manual(df, site_id)
    
    try:
        discharge_cols = [c for c in df.columns if "00060" in c and "cd" not in c.lower()]
        if not discharge_cols:
            return None
        
        discharge_col = discharge_cols[0]
        
        # Use hyswap to calculate percentiles by day of year
        percentile_df = hyswap.percentiles.calculate_variable_percentile_thresholds_by_day(
            df,
            data_column_name=discharge_col,
            percentiles=list(PERCENTILES)
        )
        
        percentile_df["site_id"] = site_id
        percentile_df = percentile_df.reset_index()
        percentile_df.rename(columns={"index": "month_day"}, inplace=True)
        
        return percentile_df
        
    except Exception as e:
        logger.debug(f"hyswap error for {site_id}: {e}")
        return calculate_percentiles_manual(df, site_id)


def calculate_percentiles_manual(df: pd.DataFrame, site_id: str) -> Optional[pd.DataFrame]:
    """Manual percentile calculation as fallback."""
    try:
        discharge_cols = [c for c in df.columns if "00060" in c and "cd" not in c.lower()]
        if not discharge_cols:
            return None
        
        col = discharge_cols[0]
        
        # Add day of year
        df = df.copy()
        df['doy'] = df.index.dayofyear
        
        results = []
        for doy in range(1, 367):
            doy_data = df[df['doy'] == doy][col].dropna()
            
            if len(doy_data) < 5:  # Need at least 5 years
                continue
            
            # Convert DOY to month-day string
            from datetime import date
            try:
                sample_date = date(2000, 1, 1) + pd.Timedelta(days=doy-1)
                month_day = sample_date.strftime("%m-%d")
            except:
                month_day = f"{doy:03d}"
            
            row = {
                'site_id': site_id,
                'month_day': month_day,
            }
            
            for p in PERCENTILES:
                row[f'p{p:02d}'] = np.percentile(doy_data, p)
            
            # Add count and year range
            row['count'] = len(doy_data)
            
            results.append(row)
        
        return pd.DataFrame(results) if results else None
        
    except Exception as e:
        logger.error(f"Manual calc error for {site_id}: {e}")
        return None


def process_site(site_id: str) -> Optional[pd.DataFrame]:
    """Process a single site: fetch history and calculate percentiles."""
    df = fetch_site_history(site_id)
    if df is None:
        return None
    
    return calculate_percentiles_hyswap(df, site_id)


def generate_state_percentiles(state_code: str, s3_client: S3Client) -> dict:
    """Generate percentiles for all sites in a state and upload to S3."""
    logger.info(f"📊 Processing state: {state_code}")
    
    sites = get_sites_for_state(state_code)
    if not sites:
        logger.warning(f"No sites found for {state_code}")
        return {"state": state_code, "sites": 0, "uploaded": False}
    
    logger.info(f"   Found {len(sites)} sites for {state_code}")
    
    # Process sites in parallel
    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_site, site): site for site in sites}
        
        for future in as_completed(futures):
            site = futures[future]
            try:
                result = future.result()
                if result is not None:
                    results.append(result)
            except Exception as e:
                logger.debug(f"Error processing {site}: {e}")
    
    if not results:
        logger.warning(f"No valid results for {state_code}")
        return {"state": state_code, "sites": 0, "uploaded": False}
    
    # Combine all site results
    combined_df = pd.concat(results, ignore_index=True)
    logger.info(f"   Generated stats for {len(results)} sites ({len(combined_df)} rows)")
    
    # Upload to S3
    success = s3_client.upload_reference_stats(combined_df, state_code)
    
    return {
        "state": state_code,
        "sites": len(results),
        "rows": len(combined_df),
        "uploaded": success
    }


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Generate USGS flow percentiles")
    parser.add_argument('--state', type=str, help='State code (e.g., VT)')
    parser.add_argument('--all', action='store_true', help='Process all states')
    parser.add_argument('--bucket', type=str, help='S3 bucket name')
    args = parser.parse_args()
    
    print("=" * 60)
    print(f"📊 USGS Flow Percentile Generator")
    print(f"   hyswap available: {HYSWAP_AVAILABLE}")
    print(f"   Started: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 60)
    
    s3_client = S3Client(bucket_name=args.bucket)
    
    if args.state:
        result = generate_state_percentiles(args.state.upper(), s3_client)
        print(f"\n✅ {result}")
    elif args.all:
        total_sites = 0
        total_rows = 0
        
        for state in ALL_STATES:
            try:
                result = generate_state_percentiles(state, s3_client)
                total_sites += result.get("sites", 0)
                total_rows += result.get("rows", 0)
            except Exception as e:
                logger.error(f"Failed state {state}: {e}")
        
        print(f"\n✅ Complete: {total_sites} sites, {total_rows:,} rows → S3")
    else:
        print("Usage: python usgs_percentiles.py --state VT")
        print("       python usgs_percentiles.py --all")
        print("       python usgs_percentiles.py --all --bucket my-bucket-name")


if __name__ == "__main__":
    main()
