"""Quality Checker - Validates data quality"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor

from .models import QualityRule, QualityCheckResult, QualityCheckType, QualityStatus

logger = logging.getLogger(__name__)


class QualityChecker:
    """Validates data quality against defined rules"""

    def __init__(self, db_connection_string: str):
        """Initialize Quality Checker"""
        self.db_connection_string = db_connection_string
        self.quality_rules: Dict[str, QualityRule] = {}
        self._load_rules_from_db()
        self.metrics = {
            "checks_passed": 0,
            "checks_failed": 0,
            "checks_warning": 0,
        }

    def _get_connection(self):
        """Get database connection"""
        return psycopg2.connect(self.db_connection_string)

    def _load_rules_from_db(self) -> None:
        """Load quality rules from database"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # Note: This assumes a quality_rules table exists
            # For now, we'll use in-memory rules
            cursor.close()
            conn.close()
            logger.info("Quality rules loaded from database")
        except Exception as e:
            logger.warning(f"Could not load rules from database: {e}")

    def add_rule(self, rule: QualityRule) -> None:
        """Add a quality rule"""
        self.quality_rules[rule.rule_id] = rule
        logger.info(f"Added quality rule: {rule.name}")

    def _check_completeness(self, data: Dict[str, Any], required_fields: List[str]) -> QualityCheckResult:
        """Check data completeness"""
        missing_fields = [f for f in required_fields if f not in data or data[f] is None]
        
        if missing_fields:
            score = (len(required_fields) - len(missing_fields)) / len(required_fields)
            return QualityCheckResult(
                status=QualityStatus.FAILED if score < 0.95 else QualityStatus.WARNING,
                score=score,
                message=f"Missing fields: {', '.join(missing_fields)}",
                affected_records=len(missing_fields)
            )
        
        return QualityCheckResult(
            status=QualityStatus.PASSED,
            score=1.0,
            message="All required fields present"
        )

    def _check_accuracy(self, data: Dict[str, Any], field: str, expected_type: type) -> QualityCheckResult:
        """Check data accuracy (type validation)"""
        if field not in data:
            return QualityCheckResult(
                status=QualityStatus.FAILED,
                score=0.0,
                message=f"Field {field} not found"
            )
        
        value = data[field]
        if not isinstance(value, expected_type):
            return QualityCheckResult(
                status=QualityStatus.FAILED,
                score=0.0,
                message=f"Field {field} has incorrect type: {type(value).__name__} (expected {expected_type.__name__})"
            )
        
        return QualityCheckResult(
            status=QualityStatus.PASSED,
            score=1.0,
            message=f"Field {field} has correct type"
        )

    def _check_consistency(self, data: Dict[str, Any], field1: str, field2: str) -> QualityCheckResult:
        """Check data consistency between two fields"""
        if field1 not in data or field2 not in data:
            return QualityCheckResult(
                status=QualityStatus.WARNING,
                score=0.5,
                message=f"One or both fields missing: {field1}, {field2}"
            )
        
        # Simple consistency check: both should have values or both should be None
        if (data[field1] is None) != (data[field2] is None):
            return QualityCheckResult(
                status=QualityStatus.WARNING,
                score=0.5,
                message=f"Inconsistent null values between {field1} and {field2}"
            )
        
        return QualityCheckResult(
            status=QualityStatus.PASSED,
            score=1.0,
            message=f"Fields {field1} and {field2} are consistent"
        )

    def _check_uniqueness(self, data_list: List[Dict[str, Any]], field: str) -> QualityCheckResult:
        """Check uniqueness of a field across records"""
        values = [d.get(field) for d in data_list if field in d]
        unique_values = set(str(v) for v in values)
        
        if len(unique_values) < len(values):
            duplicates = len(values) - len(unique_values)
            score = len(unique_values) / len(values) if values else 0
            return QualityCheckResult(
                status=QualityStatus.FAILED if score < 0.95 else QualityStatus.WARNING,
                score=score,
                message=f"Found {duplicates} duplicate values in field {field}",
                affected_records=duplicates
            )
        
        return QualityCheckResult(
            status=QualityStatus.PASSED,
            score=1.0,
            message=f"All values in field {field} are unique"
        )

    def check(
        self,
        record_id: str,
        data: Dict[str, Any],
        rule: QualityRule
    ) -> QualityCheckResult:
        """
        Execute a quality check on data
        
        Args:
            record_id: ID of the record being checked
            data: Data to check
            rule: Quality rule to apply
            
        Returns:
            QualityCheckResult
        """
        try:
            result = QualityCheckResult(
                rule_id=rule.rule_id,
                record_id=record_id
            )
            
            if rule.check_type == QualityCheckType.COMPLETENESS:
                # Check for required fields
                required_fields = ["id", "timestamp"]  # Default required fields
                result = self._check_completeness(data, required_fields)
                
            elif rule.check_type == QualityCheckType.ACCURACY:
                # Check field types
                result = self._check_accuracy(data, "id", str)
                
            elif rule.check_type == QualityCheckType.CONSISTENCY:
                # Check consistency between fields
                result = self._check_consistency(data, "created_at", "updated_at")
                
            result.rule_id = rule.rule_id
            result.record_id = record_id
            
            # Update metrics
            if result.status == QualityStatus.PASSED:
                self.metrics["checks_passed"] += 1
            elif result.status == QualityStatus.FAILED:
                self.metrics["checks_failed"] += 1
            else:
                self.metrics["checks_warning"] += 1
            
            logger.debug(f"Quality check {rule.name}: {result.status.value}")
            return result
            
        except Exception as e:
            logger.error(f"Error during quality check: {e}")
            return QualityCheckResult(
                rule_id=rule.rule_id,
                record_id=record_id,
                status=QualityStatus.FAILED,
                score=0.0,
                message=f"Check error: {str(e)}"
            )

    def check_batch(
        self,
        records: List[Dict[str, Any]],
        rules: Optional[List[QualityRule]] = None
    ) -> List[QualityCheckResult]:
        """
        Execute quality checks on a batch of records
        
        Args:
            records: List of records to check
            rules: List of rules to apply (uses all rules if None)
            
        Returns:
            List of QualityCheckResult
        """
        results = []
        rules_to_apply = rules or list(self.quality_rules.values())
        
        for record in records:
            record_id = record.get("id", "unknown")
            for rule in rules_to_apply:
                if rule.enabled:
                    result = self.check(record_id, record, rule)
                    results.append(result)
        
        return results

    def get_metrics(self) -> Dict[str, Any]:
        """Get quality check metrics"""
        total_checks = (
            self.metrics["checks_passed"] +
            self.metrics["checks_failed"] +
            self.metrics["checks_warning"]
        )
        
        return {
            **self.metrics,
            "total_checks": total_checks,
            "pass_rate": (
                self.metrics["checks_passed"] / total_checks
                if total_checks > 0 else 0
            )
        }
