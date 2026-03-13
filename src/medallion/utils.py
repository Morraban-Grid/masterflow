"""Medallion Architecture Utilities"""

import logging
import json
from typing import Dict, Any, Optional
from datetime import datetime


def setup_logging(log_level: str = "INFO") -> None:
    """Setup logging configuration"""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )


def serialize_data(data: Any) -> str:
    """Serialize data to JSON string"""
    return json.dumps(data, default=str)


def deserialize_data(data: str) -> Dict[str, Any]:
    """Deserialize JSON string to data"""
    return json.loads(data)


def get_date_partition(timestamp: Optional[datetime] = None) -> str:
    """Get date partition string (YYYY/MM/DD)"""
    if timestamp is None:
        timestamp = datetime.utcnow()
    return timestamp.strftime("%Y/%m/%d")


def get_hour_partition(timestamp: Optional[datetime] = None) -> str:
    """Get hour partition string (YYYY/MM/DD/HH)"""
    if timestamp is None:
        timestamp = datetime.utcnow()
    return timestamp.strftime("%Y/%m/%d/%H")


def validate_schema_id(schema_id: str) -> bool:
    """Validate schema ID format"""
    # Schema ID should be alphanumeric with underscores
    return schema_id.replace("_", "").isalnum() and len(schema_id) > 0


def validate_entity_type(entity_type: str) -> bool:
    """Validate entity type format"""
    # Entity type should be alphanumeric with underscores
    return entity_type.replace("_", "").isalnum() and len(entity_type) > 0


def merge_dicts(dict1: Dict[str, Any], dict2: Dict[str, Any]) -> Dict[str, Any]:
    """Merge two dictionaries"""
    result = dict1.copy()
    result.update(dict2)
    return result


def flatten_dict(data: Dict[str, Any], parent_key: str = "", sep: str = ".") -> Dict[str, Any]:
    """Flatten nested dictionary"""
    items = []
    for k, v in data.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)
