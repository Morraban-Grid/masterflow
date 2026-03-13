"""Schema Registry Data Models"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Any, Optional
from enum import Enum


class SchemaStatus(str, Enum):
    """Schema lifecycle status"""
    DRAFT = "draft"
    ACTIVE = "active"
    DEPRECATED = "deprecated"
    RETIRED = "retired"


@dataclass
class SchemaVersion:
    """Represents a specific version of a schema"""
    schema_id: str
    version: int
    definition: Dict[str, Any]
    status: SchemaStatus = SchemaStatus.ACTIVE
    created_at: datetime = field(default_factory=datetime.utcnow)
    created_by: str = "system"
    description: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "schema_id": self.schema_id,
            "version": self.version,
            "definition": self.definition,
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
            "created_by": self.created_by,
            "description": self.description,
        }


@dataclass
class Schema:
    """Represents a schema with all its versions"""
    schema_id: str
    name: str
    versions: Dict[int, SchemaVersion] = field(default_factory=dict)
    current_version: int = 1
    created_at: datetime = field(default_factory=datetime.utcnow)

    def add_version(self, version: SchemaVersion) -> None:
        """Add a new schema version"""
        self.versions[version.version] = version
        self.current_version = max(self.current_version, version.version)

    def get_version(self, version: int) -> Optional[SchemaVersion]:
        """Get a specific schema version"""
        return self.versions.get(version)

    def get_current_version(self) -> Optional[SchemaVersion]:
        """Get the current active schema version"""
        return self.versions.get(self.current_version)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "schema_id": self.schema_id,
            "name": self.name,
            "current_version": self.current_version,
            "versions": {v: sv.to_dict() for v, sv in self.versions.items()},
            "created_at": self.created_at.isoformat(),
        }
