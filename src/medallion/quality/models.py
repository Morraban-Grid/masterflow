"""Quality Checker Data Models"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Any, Optional, List
from enum import Enum
from uuid import uuid4


class QualityCheckType(str, Enum):
    """Types of quality checks"""
    COMPLETENESS = "completeness"
    ACCURACY = "accuracy"
    CONSISTENCY = "consistency"
    UNIQUENESS = "uniqueness"
    TIMELINESS = "timeliness"


class QualityStatus(str, Enum):
    """Status of quality check"""
    PASSED = "passed"
    FAILED = "failed"
    WARNING = "warning"


@dataclass
class QualityRule:
    """Represents a quality rule"""
    rule_id: str = field(default_factory=lambda: str(uuid4()))
    name: str = ""
    check_type: QualityCheckType = QualityCheckType.COMPLETENESS
    description: Optional[str] = None
    enabled: bool = True
    threshold: float = 0.95  # 95% pass rate
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "rule_id": self.rule_id,
            "name": self.name,
            "check_type": self.check_type.value,
            "description": self.description,
            "enabled": self.enabled,
            "threshold": self.threshold,
        }


@dataclass
class QualityCheckResult:
    """Represents the result of a quality check"""
    check_id: str = field(default_factory=lambda: str(uuid4()))
    rule_id: str = ""
    record_id: str = ""
    status: QualityStatus = QualityStatus.PASSED
    score: float = 1.0
    message: Optional[str] = None
    check_timestamp: datetime = field(default_factory=datetime.utcnow)
    affected_records: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "check_id": self.check_id,
            "rule_id": self.rule_id,
            "record_id": self.record_id,
            "status": self.status.value,
            "score": self.score,
            "message": self.message,
            "check_timestamp": self.check_timestamp.isoformat(),
            "affected_records": self.affected_records,
        }
