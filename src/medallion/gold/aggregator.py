"""Gold Layer Aggregator - Creates business-ready aggregated data"""

import json
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from collections import defaultdict
import boto3

from .models import GoldRecord, AggregationRule, AggregationType

logger = logging.getLogger(__name__)


class GoldAggregator:
    """Aggregates data from Silver layer for analytics"""

    def __init__(
        self,
        minio_endpoint: str,
        minio_access_key: str,
        minio_secret_key: str,
        silver_bucket: str = "silver",
        gold_bucket: str = "gold"
    ):
        """Initialize Gold Aggregator"""
        self.minio_endpoint = minio_endpoint
        self.minio_access_key = minio_access_key
        self.minio_secret_key = minio_secret_key
        self.silver_bucket = silver_bucket
        self.gold_bucket = gold_bucket
        
        # Initialize S3 client
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=f"http://{minio_endpoint}",
            aws_access_key_id=minio_access_key,
            aws_secret_access_key=minio_secret_key,
            region_name="us-east-1"
        )
        
        # Ensure gold bucket exists
        self._ensure_bucket_exists()
        
        self.aggregation_rules: Dict[str, AggregationRule] = {}
        self.metrics = {
            "records_aggregated": 0,
            "records_failed": 0,
            "aggregations_applied": 0,
        }

    def _ensure_bucket_exists(self) -> None:
        """Ensure MinIO bucket exists"""
        try:
            self.s3_client.head_bucket(Bucket=self.gold_bucket)
            logger.info(f"Bucket {self.gold_bucket} exists")
        except Exception:
            try:
                self.s3_client.create_bucket(Bucket=self.gold_bucket)
                logger.info(f"Created bucket {self.gold_bucket}")
            except Exception as e:
                logger.error(f"Error creating bucket: {e}")
                raise

    def add_aggregation_rule(self, rule: AggregationRule) -> None:
        """Add an aggregation rule"""
        self.aggregation_rules[rule.rule_id] = rule
        logger.info(f"Added aggregation rule: {rule.name}")

    def _aggregate_sum(self, records: List[Dict[str, Any]], metric_field: str) -> float:
        """Calculate sum aggregation"""
        total = 0
        for record in records:
            if metric_field in record:
                try:
                    total += float(record[metric_field])
                except (ValueError, TypeError):
                    pass
        return total

    def _aggregate_count(self, records: List[Dict[str, Any]]) -> int:
        """Calculate count aggregation"""
        return len(records)

    def _aggregate_average(self, records: List[Dict[str, Any]], metric_field: str) -> float:
        """Calculate average aggregation"""
        values = []
        for record in records:
            if metric_field in record:
                try:
                    values.append(float(record[metric_field]))
                except (ValueError, TypeError):
                    pass
        
        return sum(values) / len(values) if values else 0

    def _aggregate_min(self, records: List[Dict[str, Any]], metric_field: str) -> Optional[float]:
        """Calculate minimum aggregation"""
        values = []
        for record in records:
            if metric_field in record:
                try:
                    values.append(float(record[metric_field]))
                except (ValueError, TypeError):
                    pass
        
        return min(values) if values else None

    def _aggregate_max(self, records: List[Dict[str, Any]], metric_field: str) -> Optional[float]:
        """Calculate maximum aggregation"""
        values = []
        for record in records:
            if metric_field in record:
                try:
                    values.append(float(record[metric_field]))
                except (ValueError, TypeError):
                    pass
        
        return max(values) if values else None

    def _aggregate_distinct_count(self, records: List[Dict[str, Any]], metric_field: str) -> int:
        """Calculate distinct count aggregation"""
        distinct_values = set()
        for record in records:
            if metric_field in record:
                distinct_values.add(str(record[metric_field]))
        return len(distinct_values)

    def aggregate(
        self,
        records: List[Dict[str, Any]],
        domain: str,
        dataset: str,
        rule: AggregationRule,
        period: str = "daily"
    ) -> Optional[GoldRecord]:
        """
        Aggregate records according to rule
        
        Args:
            records: List of records to aggregate
            domain: Domain/business area
            dataset: Dataset name
            rule: Aggregation rule to apply
            period: Aggregation period (daily, weekly, monthly)
            
        Returns:
            GoldRecord or None if aggregation failed
        """
        try:
            # Group records by dimension fields
            grouped_records = defaultdict(list)
            
            for record in records:
                # Extract dimension values
                dimension_key = tuple(
                    record.get(field, "unknown")
                    for field in rule.dimension_fields
                )
                grouped_records[dimension_key].append(record)
            
            # Create aggregated records
            gold_records = []
            
            for dimension_values, group_records in grouped_records.items():
                # Build dimensions dict
                dimensions = {
                    field: value
                    for field, value in zip(rule.dimension_fields, dimension_values)
                }
                
                # Calculate metrics
                metrics = {}
                
                if rule.aggregation_type == AggregationType.SUM:
                    metrics[rule.metric_field or "value"] = self._aggregate_sum(
                        group_records, rule.metric_field or "value"
                    )
                elif rule.aggregation_type == AggregationType.COUNT:
                    metrics["count"] = self._aggregate_count(group_records)
                elif rule.aggregation_type == AggregationType.AVERAGE:
                    metrics[rule.metric_field or "value"] = self._aggregate_average(
                        group_records, rule.metric_field or "value"
                    )
                elif rule.aggregation_type == AggregationType.MIN:
                    metrics[rule.metric_field or "value"] = self._aggregate_min(
                        group_records, rule.metric_field or "value"
                    )
                elif rule.aggregation_type == AggregationType.MAX:
                    metrics[rule.metric_field or "value"] = self._aggregate_max(
                        group_records, rule.metric_field or "value"
                    )
                elif rule.aggregation_type == AggregationType.DISTINCT_COUNT:
                    metrics["distinct_count"] = self._aggregate_distinct_count(
                        group_records, rule.metric_field or "value"
                    )
                
                # Create Gold record
                gold_record = GoldRecord(
                    domain=domain,
                    dataset=dataset,
                    dimensions=dimensions,
                    metrics=metrics,
                    period=period,
                    record_count=len(group_records)
                )
                
                gold_records.append(gold_record)
            
            # Store records
            for gold_record in gold_records:
                self._store_record(gold_record)
            
            self.metrics["records_aggregated"] += len(gold_records)
            self.metrics["aggregations_applied"] += 1
            
            logger.info(f"Aggregated {len(gold_records)} records using rule {rule.name}")
            return gold_records[0] if gold_records else None
            
        except Exception as e:
            logger.error(f"Error during aggregation: {e}")
            self.metrics["records_failed"] += 1
            return None

    def _store_record(self, record: GoldRecord) -> bool:
        """Store record in MinIO"""
        try:
            # Create S3 path: /gold/{domain}/{dataset}/{date}/
            date_str = record.aggregation_timestamp.strftime("%Y/%m/%d")
            s3_key = f"gold/{record.domain}/{record.dataset}/{date_str}/{record.record_id}.json"
            
            # Store record
            self.s3_client.put_object(
                Bucket=self.gold_bucket,
                Key=s3_key,
                Body=json.dumps(record.to_dict()),
                ContentType="application/json"
            )
            
            logger.debug(f"Stored gold record {record.record_id} to {s3_key}")
            return True
            
        except Exception as e:
            logger.error(f"Error storing gold record: {e}")
            return False

    def get_metrics(self) -> Dict[str, Any]:
        """Get aggregation metrics"""
        return self.metrics.copy()
