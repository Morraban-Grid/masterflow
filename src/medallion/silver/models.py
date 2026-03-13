"""Silver Layer Data Models"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Any, Optional, List, Callable
from enum import Enum
from uuid import uuid4


class TransformationType(str, Enum):
    """Types of transformations"""
    CLEAN = "clean"
    NORMALIZE = "normalize"
    DEDUPLICATE = "deduplicate"
    ENRICH = "enrich"
    VALIDATE = "validate"


@dataclass
class TransformationRule:
    """Represents a transformation rule"""
    rule_id: str = field(default_factory=lambda: str(uuid4()))
    name: str = ""
    transformation_type: TransformationType = TransformationType.CLEAN
    description: Optional[str] = None
    enabled: bool = True
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "rule_id": self.rule_id,
            "name": self.name,
            "transformation_type": self.transformation_type.value,
            "description": self.description,
            "enabled": self.enabled,
        }


@dataclass
class SilverRecord:
    """Represents a cleaned and transformed record in Silver layer"""
    record_id: str = field(default_factory=lambda: str(uuid4()))
    bronze_record_id: str = ""
    schema_id: str = ""
    entity_type: str = ""
    data: Dict[str, Any] = field(default_factory=dict)
    transformation_timestamp: datetime = field(default_factory=datetime.utcnow)
    transformations_applied: List[str] = field(default_factory=list)
    quality_score: float = 1.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "record_id": self.record_id,
            "bronze_record_id": self.bronze_record_id,
            "schema_id": self.schema_id,
            "entity_type": self.entity_type,
            "data": self.data,
            "transformation_timestamp": self.transformation_timestamp.isoformat(),
            "transformations_applied": self.transformations_applied,
            "quality_score": self.quality_score,
        }
