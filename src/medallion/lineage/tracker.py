"""Lineage Tracker - Tracks data lineage and provenance"""

import json
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor

from .models import LineageRecord, LineageTransformation

logger = logging.getLogger(__name__)


class LineageTracker:
    """Tracks data lineage through the medallion architecture"""

    def __init__(self, db_connection_string: str):
        """Initialize Lineage Tracker"""
        self.db_connection_string = db_connection_string
        self.lineage_records: Dict[str, LineageRecord] = {}
        self.metrics = {
            "lineage_records_created": 0,
            "lineage_records_updated": 0,
            "transformations_tracked": 0,
        }

    def _get_connection(self):
        """Get database connection"""
        return psycopg2.connect(self.db_connection_string)

    def create_lineage_record(
        self,
        source_layer: str,
        source_producer: str,
        source_topic: str,
        destination_layer: str,
        destination_path: str
    ) -> LineageRecord:
        """Create a new lineage record"""
        try:
            record = LineageRecord(
                source_layer=source_layer,
                source_producer=source_producer,
                source_topic=source_topic,
                destination_layer=destination_layer,
                destination_path=destination_path
            )
            
            # Store in database
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO lineage.lineage_records
                (record_id, source_layer, source_producer, source_topic, destination_layer, destination_path)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                record.record_id,
                source_layer,
                source_producer,
                source_topic,
                destination_layer,
                destination_path
            ))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            self.lineage_records[record.record_id] = record
            self.metrics["lineage_records_created"] += 1
            
            logger.info(f"Created lineage record {record.record_id}")
            return record
            
        except Exception as e:
            logger.error(f"Error creating lineage record: {e}")
            raise

    def add_transformation(
        self,
        lineage_record_id: str,
        transformation_type: str,
        transformation_name: str,
        parameters: Dict[str, Any],
        execution_time_ms: int = 0,
        status: str = "success",
        error_message: Optional[str] = None
    ) -> LineageTransformation:
        """Add a transformation to a lineage record"""
        try:
            transformation = LineageTransformation(
                transformation_type=transformation_type,
                transformation_name=transformation_name,
                parameters=parameters,
                execution_time_ms=execution_time_ms,
                status=status,
                error_message=error_message
            )
            
            # Store in database
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO lineage.lineage_transformations
                (lineage_id, transformation_type, transformation_name, parameters, execution_time_ms, status, error_message)
                VALUES (
                    (SELECT id FROM lineage.lineage_records WHERE record_id = %s),
                    %s, %s, %s, %s, %s, %s
                )
            """, (
                lineage_record_id,
                transformation_type,
                transformation_name,
                json.dumps(parameters),
                execution_time_ms,
                status,
                error_message
            ))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            # Update in-memory record
            if lineage_record_id in self.lineage_records:
                self.lineage_records[lineage_record_id].transformations.append(transformation)
            
            self.metrics["transformations_tracked"] += 1
            
            logger.debug(f"Added transformation {transformation_name} to lineage {lineage_record_id}")
            return transformation
            
        except Exception as e:
            logger.error(f"Error adding transformation: {e}")
            raise

    def add_quality_check(
        self,
        lineage_record_id: str,
        check_type: str,
        check_name: str,
        passed: bool,
        affected_records: int = 0,
        message: Optional[str] = None
    ) -> bool:
        """Add a quality check result to a lineage record"""
        try:
            quality_check = {
                "check_type": check_type,
                "check_name": check_name,
                "passed": passed,
                "affected_records": affected_records,
                "message": message,
                "timestamp": datetime.utcnow().isoformat()
            }
            
            # Store in database
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO lineage.quality_check_results
                (lineage_id, check_type, check_name, passed, affected_records, message)
                VALUES (
                    (SELECT id FROM lineage.lineage_records WHERE record_id = %s),
                    %s, %s, %s, %s, %s
                )
            """, (
                lineage_record_id,
                check_type,
                check_name,
                passed,
                affected_records,
                message
            ))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            # Update in-memory record
            if lineage_record_id in self.lineage_records:
                self.lineage_records[lineage_record_id].quality_checks.append(quality_check)
            
            logger.debug(f"Added quality check {check_name} to lineage {lineage_record_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error adding quality check: {e}")
            return False

    def get_lineage(self, record_id: str) -> Optional[LineageRecord]:
        """Get a lineage record"""
        return self.lineage_records.get(record_id)

    def get_upstream_lineage(self, record_id: str) -> List[LineageRecord]:
        """Get upstream lineage (sources)"""
        # This would query the database to find all records that led to this one
        # For now, return empty list
        return []

    def get_downstream_lineage(self, record_id: str) -> List[LineageRecord]:
        """Get downstream lineage (destinations)"""
        # This would query the database to find all records that depend on this one
        # For now, return empty list
        return []

    def get_metrics(self) -> Dict[str, Any]:
        """Get lineage tracking metrics"""
        return self.metrics.copy()
