"""Monitoring Agent Data Models"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Any, Optional
from enum import Enum
from uuid import uuid4


class HealthStatus(str, Enum):
    """Health status"""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"


@dataclass
class MetricPoint:
    """Represents a single metric data point"""
    metric_id: str = field(default_factory=lambda: str(uuid4()))
    metric_name: str = ""
    metric_value: float = 0.0
    metric_unit: str = ""
    labels: Dict[str, str] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "metric_id": self.metric_id,
            "metric_name": self.metric_name,
            "metric_value": self.metric_value,
            "metric_unit": self.metric_unit,
            "labels": self.labels,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class HealthCheckResult:
    """Represents a health check result"""
    check_id: str = field(default_factory=lambda: str(uuid4()))
    component_name: str = ""
    status: HealthStatus = HealthStatus.HEALTHY
    message: Optional[str] = None
    response_time_ms: int = 0
    check_timestamp: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "check_id": self.check_id,
            "component_name": self.component_name,
            "status": self.status.value,
            "message": self.message,
            "response_time_ms": self.response_time_ms,
            "check_timestamp": self.check_timestamp.isoformat(),
        }
