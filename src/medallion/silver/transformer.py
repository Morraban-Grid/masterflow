"""Silver Layer Transformer - Cleans and transforms data"""

import json
import logging
from typing import Dict, Any, Optional, List, Callable
from datetime import datetime
import boto3

from .models import SilverRecord, TransformationRule, TransformationType

logger = logging.getLogger(__name__)


class SilverTransformer:
    """Transforms and cleans data from Bronze layer"""

    def __init__(
        self,
        minio_endpoint: str,
        minio_access_key: str,
        minio_secret_key: str,
        bronze_bucket: str = "bronze",
        silver_bucket: str = "silver"
    ):
        """Initialize Silver Transformer"""
        self.minio_endpoint = minio_endpoint
        self.minio_access_key = minio_access_key
        self.minio_secret_key = minio_secret_key
        self.bronze_bucket = bronze_bucket
        self.silver_bucket = silver_bucket
        
        # Initialize S3 client
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=f"http://{minio_endpoint}",
            aws_access_key_id=minio_access_key,
            aws_secret_access_key=minio_secret_key,
            region_name="us-east-1"
        )
        
        # Ensure silver bucket exists
        self._ensure_bucket_exists()
        
        self.transformation_rules: Dict[str, TransformationRule] = {}
        self.metrics = {
            "records_transformed": 0,
            "records_failed": 0,
            "transformations_applied": 0,
        }

    def _ensure_bucket_exists(self) -> None:
        """Ensure MinIO bucket exists"""
        try:
            self.s3_client.head_bucket(Bucket=self.silver_bucket)
            logger.info(f"Bucket {self.silver_bucket} exists")
        except Exception:
            try:
                self.s3_client.create_bucket(Bucket=self.silver_bucket)
                logger.info(f"Created bucket {self.silver_bucket}")
            except Exception as e:
                logger.error(f"Error creating bucket: {e}")
                raise

    def add_transformation_rule(self, rule: TransformationRule) -> None:
        """Add a transformation rule"""
        self.transformation_rules[rule.rule_id] = rule
        logger.info(f"Added transformation rule: {rule.name}")

    def _clean_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Clean data - remove nulls, trim strings"""
        cleaned = {}
        for key, value in data.items():
            if value is None:
                continue
            if isinstance(value, str):
                value = value.strip()
            cleaned[key] = value
        return cleaned

    def _normalize_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize data - standardize formats"""
        normalized = {}
        for key, value in data.items():
            # Convert keys to lowercase
            normalized_key = key.lower()
            # Standardize boolean values
            if isinstance(value, bool):
                normalized[normalized_key] = value
            elif isinstance(value, str):
                if value.lower() in ("true", "yes", "1"):
                    normalized[normalized_key] = True
                elif value.lower() in ("false", "no", "0"):
                    normalized[normalized_key] = False
                else:
                    normalized[normalized_key] = value
            else:
                normalized[normalized_key] = value
        return normalized

    def _deduplicate_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Remove duplicate entries in lists"""
        deduplicated = {}
        for key, value in data.items():
            if isinstance(value, list):
                # Remove duplicates while preserving order
                seen = set()
                unique_list = []
                for item in value:
                    if isinstance(item, dict):
                        item_str = json.dumps(item, sort_keys=True)
                    else:
                        item_str = str(item)
                    
                    if item_str not in seen:
                        seen.add(item_str)
                        unique_list.append(item)
                deduplicated[key] = unique_list
            else:
                deduplicated[key] = value
        return deduplicated

    def transform(
        self,
        bronze_record_id: str,
        bronze_data: Dict[str, Any],
        schema_id: str,
        entity_type: str,
        transformations: Optional[List[TransformationType]] = None
    ) -> Optional[SilverRecord]:
        """
        Transform a bronze record to silver record
        
        Args:
            bronze_record_id: ID of the bronze record
            bronze_data: Raw data from bronze layer
            schema_id: Schema ID
            entity_type: Type of entity
            transformations: List of transformations to apply
            
        Returns:
            SilverRecord or None if transformation failed
        """
        try:
            data = bronze_data.copy()
            transformations_applied = []
            
            # Default transformations
            if transformations is None:
                transformations = [
                    TransformationType.CLEAN,
                    TransformationType.NORMALIZE,
                    TransformationType.DEDUPLICATE
                ]
            
            # Apply transformations
            for transformation_type in transformations:
                if transformation_type == TransformationType.CLEAN:
                    data = self._clean_data(data)
                    transformations_applied.append("clean")
                    
                elif transformation_type == TransformationType.NORMALIZE:
                    data = self._normalize_data(data)
                    transformations_applied.append("normalize")
                    
                elif transformation_type == TransformationType.DEDUPLICATE:
                    data = self._deduplicate_data(data)
                    transformations_applied.append("deduplicate")
            
            # Create Silver record
            silver_record = SilverRecord(
                bronze_record_id=bronze_record_id,
                schema_id=schema_id,
                entity_type=entity_type,
                data=data,
                transformations_applied=transformations_applied,
                quality_score=1.0
            )
            
            # Store in MinIO
            self._store_record(silver_record)
            
            self.metrics["records_transformed"] += 1
            self.metrics["transformations_applied"] += len(transformations_applied)
            
            logger.debug(f"Transformed record {bronze_record_id}")
            return silver_record
            
        except Exception as e:
            logger.error(f"Error transforming record: {e}")
            self.metrics["records_failed"] += 1
            return None

    def _store_record(self, record: SilverRecord) -> bool:
        """Store record in MinIO"""
        try:
            # Create S3 path: /silver/{entity_type}/{date}/
            date_str = record.transformation_timestamp.strftime("%Y/%m/%d")
            s3_key = f"silver/{record.entity_type}/{date_str}/{record.record_id}.json"
            
            # Store record
            self.s3_client.put_object(
                Bucket=self.silver_bucket,
                Key=s3_key,
                Body=json.dumps(record.to_dict()),
                ContentType="application/json"
            )
            
            logger.debug(f"Stored silver record {record.record_id} to {s3_key}")
            return True
            
        except Exception as e:
            logger.error(f"Error storing silver record: {e}")
            return False

    def get_metrics(self) -> Dict[str, Any]:
        """Get transformation metrics"""
        return self.metrics.copy()
