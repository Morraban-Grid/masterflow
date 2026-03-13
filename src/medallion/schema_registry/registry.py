"""Schema Registry Implementation"""

import json
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor

from .models import Schema, SchemaVersion, SchemaStatus

logger = logging.getLogger(__name__)


class SchemaRegistry:
    """Manages schema registration, versioning, and validation"""

    def __init__(self, db_connection_string: str):
        """Initialize schema registry with database connection"""
        self.db_connection_string = db_connection_string
        self.schemas: Dict[str, Schema] = {}
        self._load_schemas_from_db()

    def _get_connection(self):
        """Get database connection"""
        return psycopg2.connect(self.db_connection_string)

    def _load_schemas_from_db(self) -> None:
        """Load all schemas from database on initialization"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            cursor.execute("""
                SELECT id, name, version, definition, status, created_at, created_by
                FROM medallion.schemas
                ORDER BY name, version
            """)
            
            rows = cursor.fetchall()
            for row in rows:
                schema_id = row['name']
                if schema_id not in self.schemas:
                    self.schemas[schema_id] = Schema(
                        schema_id=schema_id,
                        name=schema_id,
                        created_at=row['created_at']
                    )
                
                version = SchemaVersion(
                    schema_id=schema_id,
                    version=row['version'],
                    definition=json.loads(row['definition']),
                    status=SchemaStatus(row['status']),
                    created_at=row['created_at'],
                    created_by=row['created_by']
                )
                self.schemas[schema_id].add_version(version)
            
            cursor.close()
            conn.close()
            logger.info(f"Loaded {len(self.schemas)} schemas from database")
        except Exception as e:
            logger.error(f"Error loading schemas from database: {e}")

    def register_schema(
        self,
        schema_id: str,
        definition: Dict[str, Any],
        created_by: str = "system",
        description: Optional[str] = None
    ) -> SchemaVersion:
        """Register a new schema or new version of existing schema"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # Get next version number
            cursor.execute(
                "SELECT MAX(version) as max_version FROM medallion.schemas WHERE name = %s",
                (schema_id,)
            )
            result = cursor.fetchone()
            next_version = (result[0] or 0) + 1

            # Insert new schema version
            cursor.execute("""
                INSERT INTO medallion.schemas (name, version, definition, status, created_by)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id, created_at
            """, (schema_id, next_version, json.dumps(definition), SchemaStatus.ACTIVE.value, created_by))

            row = cursor.fetchone()
            created_at = row[1]

            conn.commit()
            cursor.close()
            conn.close()

            # Create SchemaVersion object
            version = SchemaVersion(
                schema_id=schema_id,
                version=next_version,
                definition=definition,
                status=SchemaStatus.ACTIVE,
                created_at=created_at,
                created_by=created_by,
                description=description
            )

            # Update in-memory cache
            if schema_id not in self.schemas:
                self.schemas[schema_id] = Schema(schema_id=schema_id, name=schema_id)
            self.schemas[schema_id].add_version(version)

            logger.info(f"Registered schema {schema_id} version {next_version}")
            return version

        except Exception as e:
            logger.error(f"Error registering schema: {e}")
            raise

    def get_schema(self, schema_id: str, version: Optional[int] = None) -> Optional[SchemaVersion]:
        """Get a specific schema version or current version"""
        if schema_id not in self.schemas:
            return None

        schema = self.schemas[schema_id]
        if version is None:
            return schema.get_current_version()
        return schema.get_version(version)

    def list_schemas(self) -> List[str]:
        """List all registered schema IDs"""
        return list(self.schemas.keys())

    def validate_data(self, schema_id: str, data: Dict[str, Any], version: Optional[int] = None) -> bool:
        """Validate data against schema"""
        schema_version = self.get_schema(schema_id, version)
        if not schema_version:
            logger.warning(f"Schema {schema_id} not found")
            return False

        # Basic validation: check required fields
        definition = schema_version.definition
        required_fields = definition.get("required", [])
        
        for field in required_fields:
            if field not in data:
                logger.warning(f"Missing required field: {field}")
                return False

        return True

    def deprecate_schema(self, schema_id: str, version: int) -> bool:
        """Mark a schema version as deprecated"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                UPDATE medallion.schemas
                SET status = %s
                WHERE name = %s AND version = %s
            """, (SchemaStatus.DEPRECATED.value, schema_id, version))

            conn.commit()
            cursor.close()
            conn.close()

            if schema_id in self.schemas:
                schema_version = self.schemas[schema_id].get_version(version)
                if schema_version:
                    schema_version.status = SchemaStatus.DEPRECATED

            logger.info(f"Deprecated schema {schema_id} version {version}")
            return True

        except Exception as e:
            logger.error(f"Error deprecating schema: {e}")
            return False
