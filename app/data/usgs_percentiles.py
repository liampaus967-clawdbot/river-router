#!/usr/bin/env python3
"""
USGS Flow Percentile Calculator

Based on the FGP architecture - uses hyswap to compute DOY-based percentiles
from historical daily values.

Pipeline A: Generate reference statistics
Pipeline B: Compare current flow to reference
"""

import os
import logging
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Dict, List

import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_values
import dataretrieval.nwis as nwis

# Try to import hyswap (USGS percentile library)
try:
    import hyswap
    HYSWAP_AVAILABLE = True
except ImportError:
    HYSWAP_AVAILABLE = False
    logging.warning("hyswap not installed - using manual percentile calculation")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get('DATABASE_URL',
    'postgresql://river_router:Pacific1ride@river-router-db.c6xmmyu04pdo.us-east-1.rds.amazonaws.com:5432/river_router')

# Config
PARAM_CODE = "00060"  # Discharge
START_DATE = "2000-01-01"  # ~25 years of data
PERCENTILES = (5, 10, 25, 50, 75, 90, 95)
MAX_WORKERS = 5


def get_db_connection():
    return psycopg2.connect(DATABASE_URL)


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
    """Fetch daily values for a site."""
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
        logger.error(f"hyswap error for {site_id}: {e}")
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
            
            row = {
                'site_id': site_id,
                'month_day': f"{((doy-1)//31)+1:02d}-{((doy-1)%31)+1:02d}",  # Approximate
                'doy': doy
            }
            
            for p in PERCENTILES:
                row[f'p{p:02d}'] = np.percentile(doy_data, p)
            
            results.append(row)
        
        return pd.DataFrame(results) if results else None
        
    except Exception as e:
        logger.error(f"Manual calc error for {site_id}: {e}")
        return None


def save_percentiles_to_db(df: pd.DataFrame, site_id: str) -> int:
    """Save percentiles to database."""
    if df is None or df.empty:
        return 0
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    records = []
    for _, row in df.iterrows():
        # Parse month_day to get month and day
        try:
            month_day = str(row.get('month_day', ''))
            if '-' in month_day:
                month, day = map(int, month_day.split('-'))
            else:
                continue
        except:
            continue
        
        for p in PERCENTILES:
            col = f'p{p:02d}'
            if col in row and pd.notna(row[col]):
                records.append((
                    site_id, PARAM_CODE, f'p{p:02d}',
                    month, day, float(row[col]),
                    None, None, None,  # years_of_record, begin_year, end_year
                    datetime.now(timezone.utc)
                ))
    
    if not records:
        cur.close()
        conn.close()
        return 0
    
    try:
        execute_values(cur, """
            INSERT INTO usgs_statistics 
            (site_no, param_code, stat_type, month, day, value,
             years_of_record, begin_year, end_year, updated_at)
            VALUES %s
            ON CONFLICT (site_no, param_code, stat_type, month, day) 
            DO UPDATE SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at
        """, records, page_size=10000)
        conn.commit()
        count = len(records)
    except Exception as e:
        logger.error(f"DB error saving {site_id}: {e}")
        conn.rollback()
        count = 0
    finally:
        cur.close()
        conn.close()
    
    return count


def process_site(site_id: str) -> int:
    """Process a single site: fetch history, calculate percentiles, save to DB."""
    df = fetch_site_history(site_id)
    if df is None:
        return 0
    
    percentiles_df = calculate_percentiles_hyswap(df, site_id)
    if percentiles_df is None:
        return 0
    
    return save_percentiles_to_db(percentiles_df, site_id)


def generate_state_percentiles(state_code: str) -> Dict:
    """Generate percentiles for all sites in a state."""
    logger.info(f"Processing state: {state_code}")
    
    sites = get_sites_for_state(state_code)
    if not sites:
        logger.warning(f"No sites found for {state_code}")
        return {"state": state_code, "sites": 0, "records": 0}
    
    logger.info(f"Found {len(sites)} sites for {state_code}")
    
    total_records = 0
    success_count = 0
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_site, site): site for site in sites}
        
        for future in as_completed(futures):
            site = futures[future]
            try:
                count = future.result()
                if count > 0:
                    success_count += 1
                    total_records += count
            except Exception as e:
                logger.error(f"Error processing {site}: {e}")
    
    logger.info(f"{state_code}: {success_count}/{len(sites)} sites, {total_records:,} records")
    
    return {
        "state": state_code,
        "sites_total": len(sites),
        "sites_success": success_count,
        "records": total_records
    }


def get_current_percentile(site_id: str, current_flow: float, month: int, day: int) -> Optional[Dict]:
    """
    Get the percentile for a current flow reading.
    
    Returns dict with percentile, flow_status, drought_status
    """
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Get percentile thresholds for this day
    cur.execute("""
        SELECT stat_type, value 
        FROM usgs_statistics 
        WHERE site_no = %s AND month = %s AND day = %s
        ORDER BY stat_type
    """, (site_id, month, day))
    
    rows = cur.fetchall()
    cur.close()
    conn.close()
    
    if not rows:
        return None
    
    # Build threshold dict
    thresholds = {}
    for stat_type, value in rows:
        if stat_type.startswith('p'):
            try:
                p = int(stat_type[1:])
                thresholds[p] = value
            except:
                pass
    
    if len(thresholds) < 2:
        return None
    
    # Interpolate percentile
    sorted_pcts = sorted(thresholds.keys())
    sorted_vals = [thresholds[p] for p in sorted_pcts]
    
    if current_flow <= sorted_vals[0]:
        percentile = float(sorted_pcts[0])
    elif current_flow >= sorted_vals[-1]:
        percentile = float(sorted_pcts[-1])
    else:
        percentile = float(np.interp(current_flow, sorted_vals, sorted_pcts))
    
    # Classify
    if percentile < 10:
        flow_status = "Much Below Normal"
        drought_status = "D2 - Severe Drought" if percentile < 5 else "D1 - Moderate Drought"
    elif percentile < 25:
        flow_status = "Below Normal"
        drought_status = "D0 - Abnormally Dry"
    elif percentile < 75:
        flow_status = "Normal"
        drought_status = None
    elif percentile < 90:
        flow_status = "Above Normal"
        drought_status = None
    else:
        flow_status = "Much Above Normal"
        drought_status = None
    
    return {
        "percentile": round(percentile, 1),
        "flow_status": flow_status,
        "drought_status": drought_status
    }


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Generate USGS flow percentiles")
    parser.add_argument('--state', type=str, help='State code (e.g., VT)')
    parser.add_argument('--all', action='store_true', help='Process all states')
    args = parser.parse_args()
    
    print("=" * 60)
    print("📊 USGS Flow Percentile Generator")
    print(f"   hyswap available: {HYSWAP_AVAILABLE}")
    print(f"   Started: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 60)
    
    if args.state:
        result = generate_state_percentiles(args.state.upper())
        print(f"\n✅ {result}")
    elif args.all:
        states = [
            "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
            "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
            "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
            "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
            "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"
        ]
        
        total_sites = 0
        total_records = 0
        
        for state in states:
            try:
                result = generate_state_percentiles(state)
                total_sites += result.get("sites_success", 0)
                total_records += result.get("records", 0)
            except Exception as e:
                logger.error(f"Failed state {state}: {e}")
        
        print(f"\n✅ Complete: {total_sites} sites, {total_records:,} records")
    else:
        print("Usage: python usgs_percentiles.py --state VT")
        print("       python usgs_percentiles.py --all")


if __name__ == "__main__":
    main()
