# Medallion Architecture - Phase 2 Components

This directory contains the core components of the Medallion Architecture implementation for masterflow.

## Components

### 1. Schema Registry (`schema_registry/`)
Manages data schemas and versioning across the medallion layers.

**Key Classes:**
- `SchemaRegistry`: Main registry for schema management
- `Schema`: Represents a schema with multiple versions
- `SchemaVersion`: Represents a specific version of a schema

**Features:**
- Schema registration and versioning
- Schema validation
- Schema deprecation
- In-memory caching with database persistence

### 2. Bronze Layer (`bronze/`)
Raw data ingestion from Kafka and storage in MinIO.

**Key Classes:**
- `BronzeIngester`: Consumes from Kafka and stores raw data
- `BronzeRecord`: Represents a raw record

**Features:**
- Kafka consumer integration
- Schema validation before storage
- MinIO S3-compatible storage
- Metrics collection (records ingested, bytes ingested)

### 3. Silver Layer (`silver/`)
Data cleaning and transformation.

**Key Classes:**
- `SilverTransformer`: Applies transformations to data
- `SilverRecord`: Represents a cleaned record
- `TransformationRule`: Defines transformation rules

**Features:**
- Data cleaning (remove nulls, trim strings)
- Data normalization (standardize formats)
- Deduplication (remove duplicate entries)
- Transformation tracking
- Quality scoring

### 4. Quality Checker (`quality/`)
Data quality validation and monitoring.

**Key Classes:**
- `QualityChecker`: Validates data quality
- `QualityRule`: Defines quality rules
- `QualityCheckResult`: Represents check results

**Features:**
- Completeness checks
- Accuracy checks (type validation)
- Consistency checks
- Uniqueness checks
- Batch quality checking
- Metrics collection

### 5. Gold Layer (`gold/`)
Business-ready aggregated data creation.

**Key Classes:**
- `GoldAggregator`: Creates aggregated datasets
- `GoldRecord`: Represents an aggregated record
- `AggregationRule`: Defines aggregation rules

**Features:**
- Sum, count, average, min, max aggregations
- Distinct count aggregation
- Dimension-based grouping
- Period-based aggregation (daily, weekly, monthly)
- Metrics collection

### 6. Lineage Tracker (`lineage/`)
Tracks data lineage and provenance through the pipeline.

**Key Classes:**
- `LineageTracker`: Tracks data lineage
- `LineageRecord`: Represents a lineage record
- `LineageTransformation`: Represents a transformation in lineage

**Features:**
- Lineage record creation
- Transformation tracking
- Quality check tracking
- Upstream/downstream lineage queries
- Database persistence

### 7. Monitoring Agent (`monitoring/`)
Collects metrics and performs health checks.

**Key Classes:**
- `MonitoringAgent`: Collects metrics and health checks
- `MetricPoint`: Represents a metric data point
- `HealthCheckResult`: Represents a health check result

**Features:**
- Metric recording and flushing
- Health checks for components
- Overall health status
- Alert recording
- Metrics summary

## Configuration

Configuration is managed through environment variables. See `config.py` for available options.

**Key Environment Variables:**
- `KAFKA_BROKERS`: Kafka broker addresses (default: localhost:9092)
- `MINIO_ENDPOINT`: MinIO endpoint (default: localhost:9000)
- `POSTGRES_HOST`: PostgreSQL host (default: localhost)
- `POSTGRES_DB`: PostgreSQL database (default: medallion_metadata)
- `LOG_LEVEL`: Logging level (default: INFO)

## Usage Example

```python
from medallion.config import get_config
from medallion.schema_registry import SchemaRegistry
from medallion.bronze import BronzeIngester
from medallion.silver import SilverTransformer
from medallion.quality import QualityChecker
from medallion.gold import GoldAggregator

# Get configuration
config = get_config()

# Initialize components
schema_registry = SchemaRegistry(config.db_connection_string)
bronze_ingester = BronzeIngester(
    kafka_brokers=config.kafka_brokers,
    minio_endpoint=config.minio_endpoint,
    minio_access_key=config.minio_access_key,
    minio_secret_key=config.minio_secret_key,
    schema_registry=schema_registry
)

# Register a schema
schema_def = {
    "type": "object",
    "properties": {
        "id": {"type": "string"},
        "name": {"type": "string"},
        "timestamp": {"type": "string"}
    },
    "required": ["id", "name", "timestamp"]
}
schema_registry.register_schema("user_events", schema_def)

# Ingest data
records_ingested = bronze_ingester.ingest(["user-events-topic"])
print(f"Ingested {records_ingested} records")
```

## Testing

Each component includes unit tests. Run tests with:

```bash
pytest tests/
```

## Security Notes

- Never commit credentials or sensitive configuration
- Use environment variables for all sensitive data
- Database credentials are managed through `.env` file
- MinIO credentials are managed through `.env` file
- All connections use secure protocols where available

## Performance Considerations

- Bronze Ingester batches records for efficient storage
- Silver Transformer applies transformations in-memory
- Quality Checker can process batches of records
- Gold Aggregator groups records by dimensions for efficient aggregation
- Monitoring Agent flushes metrics in batches to reduce database load

## Future Enhancements

- Spark/Flink integration for distributed processing
- Advanced lineage visualization
- Machine learning-based quality checks
- Real-time alerting system
- Data governance and compliance features
