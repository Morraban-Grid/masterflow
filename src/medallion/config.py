"""Medallion Architecture Configuration"""

import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class MedallionConfig:
    """Configuration for Medallion Architecture"""
    
    # Kafka Configuration
    kafka_brokers: str = os.getenv("KAFKA_BROKERS", "localhost:9092")
    kafka_group_id: str = os.getenv("KAFKA_GROUP_ID", "medallion-consumer")
    
    # MinIO Configuration
    minio_endpoint: str = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    minio_access_key: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key: str = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    
    # PostgreSQL Configuration
    postgres_host: str = os.getenv("POSTGRES_HOST", "localhost")
    postgres_port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    postgres_db: str = os.getenv("POSTGRES_DB", "medallion_metadata")
    postgres_user: str = os.getenv("POSTGRES_USER", "medallion_user")
    postgres_password: str = os.getenv("POSTGRES_PASSWORD", "change_me")
    
    # Medallion Configuration
    bronze_bucket: str = os.getenv("BRONZE_BUCKET", "bronze")
    silver_bucket: str = os.getenv("SILVER_BUCKET", "silver")
    gold_bucket: str = os.getenv("GOLD_BUCKET", "gold")
    
    # Logging Configuration
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    
    @property
    def db_connection_string(self) -> str:
        """Get PostgreSQL connection string"""
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}@"
            f"{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )
    
    @property
    def minio_url(self) -> str:
        """Get MinIO URL"""
        return f"http://{self.minio_endpoint}"


def get_config() -> MedallionConfig:
    """Get Medallion configuration"""
    return MedallionConfig()
