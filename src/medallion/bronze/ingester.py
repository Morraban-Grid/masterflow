"""Bronze Layer Ingester - Consumes from Kafka and stores raw data"""

import json
import logging
from typing import Optional, Callable, Dict, Any
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import boto3

from .models import BronzeRecord
from ..schema_registry import SchemaRegistry

logger = logging.getLogger(__name__)


class BronzeIngester:
    """Ingests raw data from Kafka and stores in MinIO"""

    def __init__(
        self,
        kafka_brokers: str,
        minio_endpoint: str,
        minio_access_key: str,
        minio_secret_key: str,
        schema_registry: SchemaRegistry,
        bucket_name: str = "bronze"
    ):
        """Initialize Bronze Ingester"""
        self.kafka_brokers = kafka_brokers.split(",")
        self.minio_endpoint = minio_endpoint
        self.minio_access_key = minio_access_key
        self.minio_secret_key = minio_secret_key
        self.schema_registry = schema_registry
        self.bucket_name = bucket_name
        
        # Initialize S3 client for MinIO
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=f"http://{minio_endpoint}",
            aws_access_key_id=minio_access_key,
            aws_secret_access_key=minio_secret_key,
            region_name="us-east-1"
        )
        
        # Ensure bucket exists
        self._ensure_bucket_exists()
        
        self.consumer: Optional[KafkaConsumer] = None
        self.metrics = {
            "records_ingested": 0,
            "records_failed": 0,
            "bytes_ingested": 0,
        }

    def _ensure_bucket_exists(self) -> None:
        """Ensure MinIO bucket exists"""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logger.info(f"Bucket {self.bucket_name} exists")
        except Exception:
            try:
                self.s3_client.create_bucket(Bucket=self.bucket_name)
                logger.info(f"Created bucket {self.bucket_name}")
            except Exception as e:
                logger.error(f"Error creating bucket: {e}")
                raise

    def _create_consumer(self, topics: list) -> KafkaConsumer:
        """Create Kafka consumer"""
        return KafkaConsumer(
            *topics,
            bootstrap_servers=self.kafka_brokers,
            group_id="bronze-ingester",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            max_poll_records=100,
        )

    def _store_record(self, record: BronzeRecord) -> bool:
        """Store record in MinIO"""
        try:
            # Create S3 path: /bronze/{producer}/{date}/
            date_str = record.ingestion_timestamp.strftime("%Y/%m/%d")
            s3_key = f"bronze/{record.producer_id}/{date_str}/{record.record_id}.json"
            
            # Store record
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=json.dumps(record.to_dict()),
                ContentType="application/json"
            )
            
            self.metrics["records_ingested"] += 1
            self.metrics["bytes_ingested"] += len(json.dumps(record.to_dict()))
            
            logger.debug(f"Stored record {record.record_id} to {s3_key}")
            return True
            
        except Exception as e:
            logger.error(f"Error storing record: {e}")
            self.metrics["records_failed"] += 1
            return False

    def ingest(
        self,
        topics: list,
        max_records: Optional[int] = None,
        error_callback: Optional[Callable] = None
    ) -> int:
        """
        Ingest records from Kafka topics
        
        Args:
            topics: List of Kafka topics to consume
            max_records: Maximum records to ingest (None for infinite)
            error_callback: Callback function for errors
            
        Returns:
            Number of records ingested
        """
        try:
            self.consumer = self._create_consumer(topics)
            records_processed = 0
            
            logger.info(f"Starting ingestion from topics: {topics}")
            
            for message in self.consumer:
                try:
                    # Parse message
                    data = message.value
                    schema_id = data.get("schema_id", "unknown")
                    producer_id = data.get("producer_id", "unknown")
                    
                    # Validate schema
                    if not self.schema_registry.validate_data(schema_id, data):
                        logger.warning(f"Schema validation failed for {schema_id}")
                        if error_callback:
                            error_callback(f"Schema validation failed: {schema_id}")
                        continue
                    
                    # Create Bronze record
                    record = BronzeRecord(
                        schema_id=schema_id,
                        producer_id=producer_id,
                        topic=message.topic,
                        data=data,
                        partition=message.partition,
                        offset=message.offset,
                        source_timestamp=datetime.utcnow()
                    )
                    
                    # Store record
                    if self._store_record(record):
                        records_processed += 1
                        
                        if max_records and records_processed >= max_records:
                            logger.info(f"Reached max records limit: {max_records}")
                            break
                    
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    if error_callback:
                        error_callback(str(e))
                    self.metrics["records_failed"] += 1
            
            logger.info(f"Ingestion complete. Processed {records_processed} records")
            return records_processed
            
        except Exception as e:
            logger.error(f"Error during ingestion: {e}")
            raise
        finally:
            if self.consumer:
                self.consumer.close()

    def get_metrics(self) -> Dict[str, Any]:
        """Get ingestion metrics"""
        return self.metrics.copy()
