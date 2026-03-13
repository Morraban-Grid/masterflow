"""Bronze Layer Data Models"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Any, Optional
from uuid import uuid4


@dataclass
class BronzeRecord:
    """Represents a raw record in the Bronze layer"""
    record_id: str = field(default_factory=lambda: str(uuid4()))
    schema_id: str = ""
    producer_id: str = ""
    topic: str = ""
    data: Dict[str, Any] = field(default_factory=dict)
    ingestion_timestamp: datetime = field(default_factory=datetime.utcnow)
    source_timestamp: Optional[datetime] = None
    partition: int = 0
    offset: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "record_id": self.record_id,
            "schema_id": self.schema_id,
            "producer_id": self.producer_id,
            "topic": self.topic,
            "data": self.data,
            "ingestion_timestamp": self.ingestion_timestamp.isoformat(),
            "source_timestamp": self.source_timestamp.isoformat() if self.source_timestamp else None,
            "partition": self.partition,
            "offset": self.offset,
        }
