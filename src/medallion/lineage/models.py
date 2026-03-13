"""Lineage Tracker Data Models"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Any, Optional, List
from uuid import uuid4


@dataclass
class LineageTransformation:
    """Represents a transformation in the lineage"""
    transformation_id: str = field(default_factory=lambda: str(uuid4()))
    transformation_type: str = ""
    transformation_name: str = ""
    parameters: Dict[str, Any] = field(default_factory=dict)
    execution_time_ms: int = 0
    status: str = "success"
    error_message: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "transformation_id": self.transformation_id,
            "transformation_type": self.transformation_type,
            "transformation_name": self.transformation_name,
            "parameters": self.parameters,
            "execution_time_ms": self.execution_time_ms,
            "status": self.status,
            "error_message": self.error_message,
            "created_at": self.created_at.isoformat(),
        }


@dataclass
class LineageRecord:
    """Represents a lineage record tracking data flow"""
    record_id: str = field(default_factory=lambda: str(uuid4()))
    source_layer: str = ""
    source_producer: str = ""
    source_topic: str = ""
    destination_layer: str = ""
    destination_path: str = ""
    transformations: List[LineageTransformation] = field(default_factory=list)
    quality_checks: List[Dict[str, Any]] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "record_id": self.record_id,
            "source_layer": self.source_layer,
            "source_producer": self.source_producer,
            "source_topic": self.source_topic,
            "destination_layer": self.destination_layer,
            "destination_path": self.destination_path,
            "transformations": [t.to_dict() for t in self.transformations],
            "quality_checks": self.quality_checks,
            "created_at": self.created_at.isoformat(),
        }
