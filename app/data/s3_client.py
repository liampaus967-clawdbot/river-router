"""
S3 Client for USGS Flow Data

Handles upload/download of:
- Reference statistics (Parquet, partitioned by state)
- Live conditions (JSON snapshots)
"""

import io
import json
import logging
import os
from datetime import datetime, timedelta
from typing import Optional, Dict, List

import boto3
import pandas as pd
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

# Config from environment
S3_BUCKET = os.environ.get('S3_BUCKET', 'river-router-data')
AWS_REGION = os.environ.get('AWS_REGION', 'us-east-1')

# S3 prefixes
REFERENCE_PREFIX = 'reference_stats'
LIVE_OUTPUT_PREFIX = 'live_output'


class S3Client:
    """Client for S3 operations."""

    def __init__(self, bucket_name: Optional[str] = None):
        self.s3 = boto3.client('s3', region_name=AWS_REGION)
        self.bucket = bucket_name or S3_BUCKET

    # ==================== Reference Stats (Percentiles) ====================

    def upload_reference_stats(self, df: pd.DataFrame, state_code: str) -> bool:
        """
        Upload reference statistics (percentiles) to S3 as Parquet.
        
        Path: s3://{bucket}/reference_stats/state={state}/data.parquet
        """
        key = f"{REFERENCE_PREFIX}/state={state_code}/data.parquet"

        try:
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=True)
            buffer.seek(0)

            self.s3.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=buffer.getvalue()
            )

            logger.info(f"✅ Uploaded reference stats to s3://{self.bucket}/{key}")
            return True

        except ClientError as e:
            logger.error(f"Failed to upload reference stats for {state_code}: {e}")
            return False

    def download_reference_stats(self, state_code: str) -> Optional[pd.DataFrame]:
        """Download reference statistics from S3."""
        key = f"{REFERENCE_PREFIX}/state={state_code}/data.parquet"

        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=key)
            buffer = io.BytesIO(response["Body"].read())
            df = pd.read_parquet(buffer)
            logger.info(f"Downloaded reference stats for {state_code}")
            return df

        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                logger.warning(f"Reference stats not found for {state_code}")
            else:
                logger.error(f"Failed to download reference stats: {e}")
            return None

    def list_available_states(self) -> List[str]:
        """List all states with available reference data."""
        prefix = f"{REFERENCE_PREFIX}/state="

        try:
            paginator = self.s3.get_paginator("list_objects_v2")
            states = set()

            for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix, Delimiter="/"):
                for prefix_obj in page.get("CommonPrefixes", []):
                    state = prefix_obj["Prefix"].split("=")[-1].rstrip("/")
                    states.add(state)

            return sorted(states)

        except ClientError as e:
            logger.error(f"Failed to list states: {e}")
            return []

    # ==================== Live Output ====================

    def upload_live_output(self, data: Dict) -> bool:
        """
        Upload live monitoring output to S3.
        
        Uploads to:
        - current_status.json (latest snapshot)
        - history/YYYY-MM-DDTHHMM.json (for trend detection)
        
        Format:
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
                    "state": "ME"
                },
                ...
            }
        }
        """
        timestamp = datetime.utcnow()
        
        current_key = f"{LIVE_OUTPUT_PREFIX}/current_status.json"
        history_key = f"{LIVE_OUTPUT_PREFIX}/history/{timestamp.strftime('%Y-%m-%dT%H%M')}.json"

        try:
            json_data = json.dumps(data, separators=(",", ":"))

            # Upload current snapshot
            self.s3.put_object(
                Bucket=self.bucket,
                Key=current_key,
                Body=json_data,
                ContentType="application/json",
                CacheControl="max-age=300",  # 5 min cache
            )

            # Upload to history
            self.s3.put_object(
                Bucket=self.bucket,
                Key=history_key,
                Body=json_data,
                ContentType="application/json"
            )

            logger.info(f"✅ Uploaded live output ({data.get('site_count', 0)} sites)")
            return True

        except ClientError as e:
            logger.error(f"Failed to upload live output: {e}")
            return False

    def download_live_output(self) -> Optional[Dict]:
        """Download current live output from S3."""
        key = f"{LIVE_OUTPUT_PREFIX}/current_status.json"

        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=key)
            json_data = response["Body"].read().decode("utf-8")
            return json.loads(json_data)

        except ClientError as e:
            logger.error(f"Failed to download live output: {e}")
            return None

    def list_historical_snapshots(self, hours: int = 48) -> List[str]:
        """List S3 keys for snapshots in the time window."""
        prefix = f"{LIVE_OUTPUT_PREFIX}/history/"
        cutoff_time = datetime.utcnow() - timedelta(hours=hours)

        try:
            paginator = self.s3.get_paginator("list_objects_v2")
            keys = []

            for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    filename = key.split("/")[-1]
                    
                    if not filename.endswith(".json"):
                        continue

                    try:
                        timestamp_str = filename.replace(".json", "")
                        timestamp = datetime.strptime(timestamp_str, "%Y-%m-%dT%H%M")

                        if timestamp >= cutoff_time:
                            keys.append((timestamp, key))
                    except ValueError:
                        continue

            keys.sort(key=lambda x: x[0])
            return [key for _, key in keys]

        except ClientError as e:
            logger.error(f"Failed to list historical snapshots: {e}")
            return []

    def download_historical_snapshot(self, key: str) -> Optional[Dict]:
        """Download a single historical JSON snapshot."""
        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=key)
            json_data = response["Body"].read().decode("utf-8")
            return json.loads(json_data)

        except (ClientError, json.JSONDecodeError) as e:
            logger.warning(f"Failed to download snapshot {key}: {e}")
            return None

    def get_site_history(self, site_id: str, hours: int = 48) -> List[Dict]:
        """
        Get flow history for a single site from historical snapshots.
        
        Returns list of {timestamp, flow, temp} dicts.
        """
        keys = self.list_historical_snapshots(hours=hours)
        history = []

        for key in keys:
            snapshot = self.download_historical_snapshot(key)
            if not snapshot:
                continue

            # Parse timestamp from key
            filename = key.split("/")[-1]
            try:
                timestamp_str = filename.replace(".json", "")
                timestamp = datetime.strptime(timestamp_str, "%Y-%m-%dT%H%M")
            except ValueError:
                continue

            # Get site data
            sites = snapshot.get("sites", {})
            site_data = sites.get(site_id)
            
            if site_data and site_data.get("flow") is not None:
                history.append({
                    "timestamp": timestamp,
                    "flow": site_data["flow"],
                    "temp": site_data.get("water_temp")
                })

        return history
