"""Gold Layer Data Models"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Any, Optional, List
from enum import Enum
from uuid import uuid4


class AggregationType(str, Enum):
    """Types of aggregations"""
    SUM = "sum"
    COUNT = "count"
    AVERAGE = "average"
    MIN = "min"
    MAX = "max"
    DISTINCT_COUNT = "distinct_count"


@dataclass
class AggregationRule:
    """Represents an aggregation rule"""
    rule_id: str = field(default_factory=lambda: str(uuid4()))
    name: str = ""
    aggregation_type: AggregationType = AggregationType.COUNT
    dimension_fields: List[str] = field(default_factory=list)
    metric_field: Optional[str] = None
    description: Optional[str] = None
    enabled: bool = True
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "rule_id": self.rule_id,
            "name": self.name,
            "aggregation_type": self.aggregation_type.value,
            "dimension_fields": self.dimension_fields,
            "metric_field": self.metric_field,
            "description": self.description,
            "enabled": self.enabled,
        }


@dataclass
class GoldRecord:
    """Represents an aggregated record in Gold layer"""
    record_id: str = field(default_factory=lambda: str(uuid4()))
    domain: str = ""
    dataset: str = ""
    dimensions: Dict[str, Any] = field(default_factory=dict)
    metrics: Dict[str, Any] = field(default_factory=dict)
    aggregation_timestamp: datetime = field(default_factory=datetime.utcnow)
    period: str = "daily"  # daily, weekly, monthly
    record_count: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "record_id": self.record_id,
            "domain": self.domain,
            "dataset": self.dataset,
            "dimensions": self.dimensions,
            "metrics": self.metrics,
            "aggregation_timestamp": self.aggregation_timestamp.isoformat(),
            "period": self.period,
            "record_count": self.record_count,
        }
