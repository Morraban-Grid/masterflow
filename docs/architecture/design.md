# Medallion Architecture Design Document

## Overview

The Medallion Architecture for masterflow implements a three-layer data organization pattern (Bronze-Silver-Gold) that enables progressive data refinement from raw ingestion through business-ready analytics. This design leverages open-source technologies (Apache Kafka, Apache Spark/Flink, MinIO/S3, PostgreSQL) to create a scalable, maintainable data lake that integrates seamlessly with the existing masterflow ETL infrastructure.

### Design Principles

1. **Immutability**: Bronze layer maintains write-once semantics; data is never modified after ingestion
2. **Progressive Refinement**: Each layer adds value through cleaning, transformation, and aggregation
3. **Traceability**: Complete lineage tracking enables root cause analysis and compliance auditing
4. **Scalability**: Horizontal scaling through partitioning and distributed processing
5. **Resilience**: Automatic retry, dead-letter queues, and checkpoint-based recovery
6. **Cost Efficiency**: Open-source tools with minimal infrastructure overhead

### Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         MEDALLION ARCHITECTURE                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────┐      ┌──────────────┐      ┌──────────────┐              │
│  │ Go Producer  │─────▶│   Kafka      │─────▶│   Bronze     │              │
│  │  (Events)    │      │   Topics     │      │   Layer      │              │
│  └──────────────┘      └──────────────┘      └──────────────┘              │
│                              │                      │                       │
│                              │                      ▼                       │
│                              │              ┌──────────────┐               │
│                              │              │ Schema       │               │
│                              │              │ Validator    │               │
│                              │              └──────────────┘               │
│                              │                      │                       │
│                              │                      ▼                       │
│                              │              ┌──────────────┐               │
│                              │              │ Lineage      │               │
│                              │              │ Tracker      │               │
│                              │              └──────────────┘               │
│                              │                      │                       │
│                              ▼                      ▼                       │
│                        ┌──────────────┐      ┌──────────────┐              │
│                        │ Silver Layer │◀─────│ Transformer  │              │
│                        │ (Cleaned)    │      │ (Spark/Flink)│              │
│                        └──────────────┘      └──────────────┘              │
│                              │                      │                       │
│                              ▼                      ▼                       │
│                        ┌──────────────┐      ┌──────────────┐              │
│                        │ Quality      │      │ Deduplicator │              │
│                        │ Checker      │      │ & Enricher   │              │
│                        └──────────────┘      └──────────────┘              │
│                              │                      │                       │
│                              ▼                      ▼                       │
│                        ┌──────────────┐      ┌──────────────┐              │
│                        │ Gold Layer   │◀─────│ Aggregator   │              │
│                        │ (Analytics)  │      │ (Spark/Flink)│              │
│                        └──────────────┘      └──────────────┘              │
│                              │                      │                       │
│                              ▼                      ▼                       │
│                        ┌──────────────┐      ┌──────────────┐              │
│                        │ Data         │      │ Monitoring   │              │
│                        │ Warehouse    │      │ Agent        │              │
│                        │ Connector    │      └──────────────┘              │
│                        └──────────────┘                                     │
│                              │                                              │
│                              ▼                                              │
│                        ┌──────────────┐                                     │
│                        │ SQL Query    │                                     │
│                        │ Interface    │                                     │
│                        └──────────────┘                                     │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    S3-Compatible Data Lake (MinIO/S3)               │   │
│  │  ├── /bronze/{producer}/{date}/                                    │   │
│  │  ├── /silver/{entity_type}/{date}/                                 │   │
│  │  ├── /gold/{domain}/{dataset}/{date}/                              │   │
│  │  └── /quarantine/{layer}/{reason}/{date}/                          │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    Metadata & Lineage Store (PostgreSQL)            │   │
│  │  ├── schemas (version, definition, status)                         │   │
│  │  ├── lineage (source, transformations, destination)                │   │
│  │  ├── quality_checks (results, failures, alerts)                    │   │
│  │  └── access_policies (users, datasets, permissions)                │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    Monitoring & Observability                       │   │
│  │  ├── Prometheus (metrics collection)                               │   │
│  │  ├── Grafana (dashboards)                                          │   │
│  │  ├── ELK Stack (logging)                                           │   │
│  │  └── Alert Manager (notifications)                                 │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Architecture

### Data Flow

1. **Ingestion**: Go producers publish events to Kafka topics with schema identifiers
2. **Bronze Layer**: Raw events are persisted to S3-compatible storage with immutable identifiers
3. **Validation**: Schema validation and quality checks ensure data conformance
4. **Silver Layer**: Data is cleaned, deduplicated, and standardized through Spark/Flink jobs
5. **Enrichment**: Business logic transformations and data enrichment are applied
6. **Quality Assurance**: Automated quality checks validate Silver layer data
7. **Gold Layer**: Aggregated, business-ready datasets are created for analytics
8. **Exposure**: Data Warehouse Connector exposes Gold layer through SQL interfaces
9. **Monitoring**: Metrics, logs, and alerts track pipeline health and performance
10. **Lineage**: Complete transformation history is recorded for compliance and debugging

### Integration with Masterflow

The medallion architecture integrates with existing masterflow components through:

- **Scheduler Integration**: Medallion Orchestrator coordinates with masterflow schedulers
- **Data Connectors**: Existing masterflow connectors can read from any medallion layer
- **Error Handling**: Follows masterflow error handling patterns and logging standards
- **Adapters**: Provides adapters for consuming medallion data in existing pipelines
- **Backward Compatibility**: Existing pipelines continue operating without interruption

## Components and Interfaces

### 1. Go Producer

**Purpose**: Generates and publishes data events to Kafka topics

**Responsibilities**:
- Generate events with consistent schema
- Publish to Kafka with schema identifier
- Implement retry logic with exponential backoff
- Log failures with event metadata

**Interface**:
```go
type Event struct {
    SchemaID    string                 `json:"schema_id"`
    Timestamp   time.Time              `json:"timestamp"`
    ProducerID  string                 `json:"producer_id"`
    Data        map[string]interface{} `json:"data"`
}

type Producer interface {
    Publish(ctx context.Context, topic string, event Event) error
    PublishBatch(ctx context.Context, topic string, events []Event) error
}
```

**Configuration**:
```yaml
producer:
  kafka_brokers: ["kafka-1:9092", "kafka-2:9092", "kafka-3:9092"]
  schema_registry_url: "http://schema-registry:8081"
  retry_attempts: 3
  retry_backoff_ms: 100
  batch_size: 1000
  flush_interval_ms: 5000
```

### 2. Kafka Cluster

**Purpose**: Distributed message broker for event streaming

**Configuration**:
- **Brokers**: 3-node cluster for fault tolerance
- **Topics**: One topic per producer/entity type
- **Partitions**: Scaled by expected throughput (min 3 per topic)
- **Replication Factor**: 3 for production
- **Retention**: Configurable per topic (default: 7 days for Bronze ingestion)

**Topic Naming Convention**:
```
{environment}.{producer_name}.{entity_type}
prod.user-service.events
prod.order-service.transactions
```

**Broker Configuration**:
```properties
# Replication and Durability
min.insync.replicas=2
default.replication.factor=3
unclean.leader.election.enable=false

# Performance
num.network.threads=8
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400

# Retention
log.retention.hours=168
log.segment.bytes=1073741824
log.cleanup.policy=delete
```

### 3. Schema Registry

**Purpose**: Centralized schema management and versioning

**Responsibilities**:
- Register and version schemas
- Validate data against schemas
- Support schema evolution
- Prevent deprecated schema usage

**Schema Storage** (PostgreSQL):
```sql
CREATE TABLE schemas (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    version INT NOT NULL,
    definition JSONB NOT NULL,
    status VARCHAR(50) DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deprecated_at TIMESTAMP,
    UNIQUE(name, version)
);

CREATE TABLE schema_evolution (
    id SERIAL PRIMARY KEY,
    from_version INT NOT NULL,
    to_version INT NOT NULL,
    is_backward_compatible BOOLEAN,
    breaking_changes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**Interface**:
```python
class SchemaRegistry:
    def register_schema(self, name: str, definition: dict) -> int:
        """Register a new schema version"""
        
    def get_schema(self, name: str, version: int) -> dict:
        """Retrieve schema definition"""
        
    def validate(self, data: dict, schema_id: str) -> bool:
        """Validate data against schema"""
        
    def check_compatibility(self, from_version: int, to_version: int) -> bool:
        """Check backward compatibility"""
```

### 4. Bronze Layer Ingester

**Purpose**: Consume Kafka events and persist to Bronze layer

**Responsibilities**:
- Consume from Kafka topics
- Validate schema identifiers
- Write to S3 with immutable identifiers
- Record lineage information
- Implement retry logic

**Storage Structure**:
```
s3://data-lake/bronze/
├── {producer_name}/
│   ├── {date}/
│   │   ├── {hour}/
│   │   │   ├── part-00000.parquet
│   │   │   ├── part-00001.parquet
│   │   │   └── _SUCCESS
│   │   └── _metadata.json
│   └── _schema.json
```

**Metadata File** (`_metadata.json`):
```json
{
  "producer_id": "user-service",
  "ingestion_timestamp": "2024-01-15T10:30:00Z",
  "record_count": 10000,
  "schema_version": 2,
  "kafka_topic": "prod.user-service.events",
  "kafka_partition": 0,
  "kafka_offset_start": 1000000,
  "kafka_offset_end": 1010000,
  "file_format": "parquet",
  "compression": "snappy"
}
```

**Implementation** (PySpark):
```python
class BronzeIngester:
    def __init__(self, kafka_brokers, s3_client, schema_registry):
        self.kafka_brokers = kafka_brokers
        self.s3_client = s3_client
        self.schema_registry = schema_registry
        
    def ingest(self, topic: str, partition: int):
        """Consume and persist Bronze layer data"""
        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_brokers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .load()
        
        # Validate schema
        df = df.filter(self.validate_schema_udf(col("value")))
        
        # Add immutable identifier and metadata
        df = df.withColumn("record_id", F.expr("uuid()")) \
               .withColumn("ingestion_ts", F.current_timestamp()) \
               .withColumn("producer_id", F.lit(self.producer_id))
        
        # Write to S3
        query = df.writeStream \
            .format("parquet") \
            .option("path", f"s3://data-lake/bronze/{self.producer_id}/") \
            .option("checkpointLocation", f"s3://checkpoints/bronze/{topic}/") \
            .partitionBy("date") \
            .start()
        
        return query
```

### 5. Silver Layer Transformer

**Purpose**: Clean, deduplicate, and standardize Bronze layer data

**Responsibilities**:
- Apply data cleaning rules
- Detect and remove duplicates
- Apply business logic transformations
- Maintain referential integrity
- Route failed records to quarantine

**Transformation Rules** (YAML):
```yaml
transformations:
  - entity_type: "user"
    rules:
      - type: "null_handling"
        field: "email"
        action: "drop_row"
      - type: "type_conversion"
        field: "age"
        target_type: "int"
        default: 0
      - type: "format_standardization"
        field: "phone"
        pattern: "^\\d{10}$"
      - type: "calculated_field"
        name: "full_name"
        expression: "concat(first_name, ' ', last_name)"
      - type: "enrichment"
        field: "country"
        lookup_table: "countries"
        join_key: "country_code"
```

**Deduplication Strategy**:
```python
class Deduplicator:
    def deduplicate(self, df, key_columns: List[str], 
                   timestamp_column: str = "ingestion_ts"):
        """Remove duplicates, keeping most recent record"""
        window_spec = Window.partitionBy(*key_columns) \
                           .orderBy(col(timestamp_column).desc())
        
        return df.withColumn("row_num", 
                            row_number().over(window_spec)) \
                .filter(col("row_num") == 1) \
                .drop("row_num")
```

**Storage Structure**:
```
s3://data-lake/silver/
├── {entity_type}/
│   ├── {date}/
│   │   ├── {hour}/
│   │   │   ├── part-00000.parquet
│   │   │   └── part-00001.parquet
│   │   └── _metadata.json
│   └── _schema.json
```

### 6. Quality Checker

**Purpose**: Validate Silver layer data quality

**Responsibilities**:
- Execute configured quality checks
- Record check results
- Route failed records to quarantine
- Publish alerts for critical failures
- Prevent promotion of failed datasets

**Quality Check Types**:
```python
class QualityCheck:
    """Base class for quality checks"""
    
    def execute(self, df: DataFrame) -> QualityResult:
        raise NotImplementedError

class CompletenessCheck(QualityCheck):
    """Verify required fields are present"""
    def __init__(self, required_fields: List[str]):
        self.required_fields = required_fields
    
    def execute(self, df: DataFrame) -> QualityResult:
        for field in self.required_fields:
            null_count = df.filter(col(field).isNull()).count()
            if null_count > 0:
                return QualityResult(
                    passed=False,
                    check_type="completeness",
                    affected_records=null_count,
                    message=f"{field} has {null_count} null values"
                )
        return QualityResult(passed=True, check_type="completeness")

class AccuracyCheck(QualityCheck):
    """Verify data accuracy against rules"""
    def __init__(self, rules: List[str]):
        self.rules = rules
    
    def execute(self, df: DataFrame) -> QualityResult:
        for rule in self.rules:
            failed_count = df.filter(~expr(rule)).count()
            if failed_count > 0:
                return QualityResult(
                    passed=False,
                    check_type="accuracy",
                    affected_records=failed_count,
                    message=f"Rule '{rule}' failed for {failed_count} records"
                )
        return QualityResult(passed=True, check_type="accuracy")

class ConsistencyCheck(QualityCheck):
    """Verify referential integrity"""
    def __init__(self, foreign_key: str, reference_table: str):
        self.foreign_key = foreign_key
        self.reference_table = reference_table
    
    def execute(self, df: DataFrame) -> QualityResult:
        # Implementation for referential integrity checks
        pass
```

**Quality Check Configuration**:
```yaml
quality_checks:
  - entity_type: "user"
    checks:
      - type: "completeness"
        fields: ["user_id", "email", "created_at"]
        critical: true
      - type: "accuracy"
        rules:
          - "age >= 0 AND age <= 150"
          - "email LIKE '%@%.%'"
        critical: true
      - type: "consistency"
        foreign_key: "country_id"
        reference_table: "countries"
        critical: false
    timeout_minutes: 30
```

**Quarantine Dataset**:
```
s3://data-lake/quarantine/
├── {layer}/
│   ├── {failure_reason}/
│   │   ├── {date}/
│   │   │   ├── part-00000.parquet
│   │   │   └── _metadata.json
```

### 7. Gold Layer Aggregator

**Purpose**: Create aggregated, business-ready datasets

**Responsibilities**:
- Apply business logic aggregations
- Maintain data consistency
- Include aggregation metadata
- Prevent partial writes
- Retain previous version on failure

**Aggregation Definitions** (YAML):
```yaml
aggregations:
  - domain: "sales"
    dataset_name: "daily_revenue"
    source_entity: "transactions"
    aggregations:
      - type: "sum"
        field: "amount"
        alias: "total_revenue"
      - type: "count"
        alias: "transaction_count"
      - type: "avg"
        field: "amount"
        alias: "avg_transaction"
    group_by: ["date", "region", "product_category"]
    filters:
      - "status = 'completed'"
      - "amount > 0"
    schedule: "daily"
    
  - domain: "customer"
    dataset_name: "customer_segments"
    source_entity: "customers"
    aggregations:
      - type: "custom_sql"
        expression: |
          CASE 
            WHEN lifetime_value > 10000 THEN 'VIP'
            WHEN lifetime_value > 1000 THEN 'Premium'
            ELSE 'Standard'
          END as segment
    group_by: ["segment"]
    schedule: "weekly"
```

**Implementation** (PySpark):
```python
class GoldAggregator:
    def aggregate(self, silver_df: DataFrame, 
                 aggregation_config: dict) -> DataFrame:
        """Create Gold layer aggregations"""
        
        # Apply filters
        for filter_expr in aggregation_config.get("filters", []):
            silver_df = silver_df.filter(expr(filter_expr))
        
        # Apply aggregations
        agg_specs = []
        for agg in aggregation_config["aggregations"]:
            if agg["type"] == "sum":
                agg_specs.append(F.sum(agg["field"]).alias(agg["alias"]))
            elif agg["type"] == "count":
                agg_specs.append(F.count("*").alias(agg["alias"]))
            elif agg["type"] == "avg":
                agg_specs.append(F.avg(agg["field"]).alias(agg["alias"]))
        
        # Group and aggregate
        gold_df = silver_df.groupBy(*aggregation_config["group_by"]) \
                          .agg(*agg_specs)
        
        # Add metadata
        gold_df = gold_df.withColumn("aggregation_ts", F.current_timestamp()) \
                        .withColumn("source_version", F.lit(self.silver_version)) \
                        .withColumn("record_count", F.count("*").over(Window.partitionBy()))
        
        return gold_df
```

**Storage Structure**:
```
s3://data-lake/gold/
├── {domain}/
│   ├── {dataset_name}/
│   │   ├── {date}/
│   │   │   ├── part-00000.parquet
│   │   │   └── _metadata.json
│   │   └── _current (symlink to latest)
```

### 8. Lineage Tracker

**Purpose**: Record data provenance and transformation history

**Responsibilities**:
- Record source, transformations, and destination
- Maintain chain of custody
- Enable root cause analysis
- Support compliance auditing
- Archive historical lineage

**Lineage Storage** (PostgreSQL):
```sql
CREATE TABLE lineage_records (
    id SERIAL PRIMARY KEY,
    record_id UUID NOT NULL,
    source_layer VARCHAR(50) NOT NULL,
    source_producer VARCHAR(255),
    source_topic VARCHAR(255),
    destination_layer VARCHAR(50) NOT NULL,
    destination_path VARCHAR(1024),
    transformations JSONB,
    quality_checks JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_record_id (record_id),
    INDEX idx_created_at (created_at)
);

CREATE TABLE lineage_transformations (
    id SERIAL PRIMARY KEY,
    lineage_id INT REFERENCES lineage_records(id),
    transformation_type VARCHAR(100),
    transformation_name VARCHAR(255),
    parameters JSONB,
    execution_time_ms INT,
    status VARCHAR(50),
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**Lineage Query Interface**:
```python
class LineageTracker:
    def get_lineage(self, record_id: str, 
                   date_range: Tuple[date, date] = None) -> LineageChain:
        """Retrieve complete transformation history"""
        
    def trace_to_source(self, record_id: str) -> List[LineageRecord]:
        """Trace record back to original source"""
        
    def trace_to_destination(self, record_id: str) -> List[LineageRecord]:
        """Trace record forward to all destinations"""
        
    def get_affected_records(self, transformation_id: str) -> List[str]:
        """Find all records affected by a transformation"""
```

### 9. Access Control Manager

**Purpose**: Enforce access control policies

**Responsibilities**:
- Verify access requests
- Log access denials
- Enforce column-level access control
- Trigger security alerts

**Access Policy Storage** (PostgreSQL):
```sql
CREATE TABLE access_policies (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL,
    dataset_path VARCHAR(1024) NOT NULL,
    access_level VARCHAR(50) NOT NULL, -- 'read', 'write', 'admin'
    column_restrictions JSONB, -- null = all columns allowed
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP,
    UNIQUE(user_id, dataset_path)
);

CREATE TABLE access_audit_log (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL,
    dataset_path VARCHAR(1024) NOT NULL,
    access_type VARCHAR(50),
    allowed BOOLEAN,
    reason VARCHAR(255),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_user_timestamp (user_id, timestamp)
);
```

**Implementation**:
```python
class AccessControlManager:
    def check_access(self, user_id: str, dataset_path: str, 
                    access_type: str) -> AccessDecision:
        """Verify access request"""
        policy = self.get_policy(user_id, dataset_path)
        
        if not policy:
            self.log_denial(user_id, dataset_path, "no_policy")
            return AccessDecision(allowed=False, reason="no_policy")
        
        if access_type not in policy.access_level:
            self.log_denial(user_id, dataset_path, "insufficient_permissions")
            return AccessDecision(allowed=False, reason="insufficient_permissions")
        
        if policy.expires_at and policy.expires_at < datetime.now():
            self.log_denial(user_id, dataset_path, "policy_expired")
            return AccessDecision(allowed=False, reason="policy_expired")
        
        return AccessDecision(allowed=True, column_restrictions=policy.column_restrictions)
    
    def enforce_column_restrictions(self, df: DataFrame, 
                                   restrictions: List[str]) -> DataFrame:
        """Apply column-level access control"""
        allowed_columns = [col for col in df.columns 
                          if col not in restrictions]
        return df.select(*allowed_columns)
```

### 10. Monitoring Agent

**Purpose**: Collect and report pipeline metrics

**Responsibilities**:
- Collect throughput, latency, error rate metrics
- Store metrics in time-series database
- Publish alerts on failures
- Maintain real-time dashboards
- Continue operating on agent failure

**Metrics Collection** (Prometheus):
```python
from prometheus_client import Counter, Histogram, Gauge

# Throughput metrics
records_ingested = Counter(
    'medallion_records_ingested_total',
    'Total records ingested',
    ['layer', 'producer', 'status']
)

# Latency metrics
processing_latency = Histogram(
    'medallion_processing_latency_seconds',
    'Processing latency in seconds',
    ['layer', 'operation'],
    buckets=(0.1, 0.5, 1, 5, 10, 30, 60)
)

# Error metrics
processing_errors = Counter(
    'medallion_processing_errors_total',
    'Total processing errors',
    ['layer', 'error_type']
)

# Queue depth
queue_depth = Gauge(
    'medallion_queue_depth',
    'Current queue depth',
    ['layer', 'queue_type']
)
```

**Alert Rules** (Prometheus):
```yaml
groups:
  - name: medallion_alerts
    rules:
      - alert: HighErrorRate
        expr: rate(medallion_processing_errors_total[5m]) > 0.01
        for: 5m
        annotations:
          summary: "High error rate in {{ $labels.layer }}"
          
      - alert: LowThroughput
        expr: rate(medallion_records_ingested_total[5m]) < 100
        for: 10m
        annotations:
          summary: "Low throughput in {{ $labels.layer }}"
          
      - alert: HighLatency
        expr: histogram_quantile(0.95, medallion_processing_latency_seconds) > 300
        for: 5m
        annotations:
          summary: "High latency in {{ $labels.layer }}"
```

### 11. Medallion Orchestrator

**Purpose**: Coordinate data flow and transformations

**Responsibilities**:
- Coordinate with masterflow schedulers
- Implement retry logic with exponential backoff
- Route failed data to dead-letter queues
- Manage checkpoints and recovery
- Maintain transaction logs

**Orchestration Logic**:
```python
class MedallionOrchestrator:
    def __init__(self, max_retries=3, backoff_base=100):
        self.max_retries = max_retries
        self.backoff_base = backoff_base
    
    def execute_with_retry(self, operation, *args, **kwargs):
        """Execute operation with exponential backoff retry"""
        for attempt in range(self.max_retries):
            try:
                return operation(*args, **kwargs)
            except Exception as e:
                if attempt == self.max_retries - 1:
                    self.route_to_dlq(operation, args, kwargs, e)
                    raise
                
                backoff_ms = self.backoff_base * (2 ** attempt)
                time.sleep(backoff_ms / 1000)
    
    def route_to_dlq(self, operation, args, kwargs, error):
        """Route failed operation to dead-letter queue"""
        dlq_record = {
            "operation": operation.__name__,
            "args": args,
            "kwargs": kwargs,
            "error": str(error),
            "timestamp": datetime.now().isoformat(),
            "status": "pending_replay"
        }
        self.dlq_store.put(dlq_record)
    
    def replay_dlq(self, dlq_record):
        """Replay operation from dead-letter queue"""
        operation = getattr(self, dlq_record["operation"])
        return self.execute_with_retry(
            operation,
            *dlq_record["args"],
            **dlq_record["kwargs"]
        )
```

**Checkpoint Management**:
```python
class CheckpointManager:
    def save_checkpoint(self, layer: str, offset: int, 
                       metadata: dict):
        """Save processing checkpoint"""
        checkpoint = {
            "layer": layer,
            "offset": offset,
            "metadata": metadata,
            "timestamp": datetime.now().isoformat()
        }
        self.checkpoint_store.put(checkpoint)
    
    def get_last_checkpoint(self, layer: str) -> dict:
        """Retrieve last successful checkpoint"""
        return self.checkpoint_store.get_latest(layer)
    
    def resume_from_checkpoint(self, layer: str):
        """Resume processing from last checkpoint"""
        checkpoint = self.get_last_checkpoint(layer)
        return checkpoint["offset"] if checkpoint else 0
```



## Data Storage Design

### S3-Compatible Storage Architecture

The medallion architecture uses S3-compatible storage (MinIO for on-premises, AWS S3 for cloud) as the central data lake.

**Storage Structure**:
```
s3://data-lake/
├── bronze/                          # Raw, immutable data
│   ├── {producer_name}/
│   │   ├── {date}/
│   │   │   ├── {hour}/
│   │   │   │   ├── part-00000.parquet
│   │   │   │   ├── part-00001.parquet
│   │   │   │   └── _SUCCESS
│   │   │   └── _metadata.json
│   │   └── _schema.json
│   └── _manifest.json
│
├── silver/                          # Cleaned, standardized data
│   ├── {entity_type}/
│   │   ├── {date}/
│   │   │   ├── {hour}/
│   │   │   │   ├── part-00000.parquet
│   │   │   │   └── part-00001.parquet
│   │   │   └── _metadata.json
│   │   └── _schema.json
│   └── _manifest.json
│
├── gold/                            # Business-ready aggregations
│   ├── {domain}/
│   │   ├── {dataset_name}/
│   │   │   ├── {date}/
│   │   │   │   ├── part-00000.parquet
│   │   │   │   └─
_metadata.json
│   │   └── _current (symlink)
│   └── _manifest.json
│
├── quarantine/                      # Failed records for review
│   ├── bronze/
│   │   ├── {failure_reason}/
│   │   │   └── {date}/
│   │   │       └── part-00000.parquet
│   ├── silver/
│   │   ├── {failure_reason}/
│   │   │   └── {date}/
│   │   │       └── part-00000.parquet
│   └── _manifest.json
│
└── checkpoints/                     # Spark/Flink checkpoints
    ├── bronze/
    │   └── {topic}/
    ├── silver/
    │   └── {entity_type}/
    └── gold/
        └── {dataset_name}/
```

### Parquet File Format

All data is stored in Apache Parquet format with Snappy compression for optimal storage and query performance.

**Parquet Configuration**:
```python
parquet_config = {
    "format": "parquet",
    "compression": "snappy",
    "partition_by": ["date"],  # Partition by date for query optimization
    "coalesce": 10,  # Target 10 files per partition
    "mode": "overwrite"  # Overwrite mode for idempotency
}
```

## Metadata and Lineage Store

### PostgreSQL Schema

The medallion architecture uses PostgreSQL as the metadata and lineage store.

**Core Tables**:

1. **schemas** - Schema definitions and versioning
2. **lineage_records** - Data lineage tracking
3. **quality_check_results** - Quality check execution results
4. **access_policies** - Access control policies
5. **access_audit_log** - Access audit trail
6. **configuration** - System configuration

**Database Connection**:
```python
import psycopg2
from psycopg2.pool import SimpleConnectionPool

class MetadataStore:
    def __init__(self, host, port, database, user, password):
        self.pool = SimpleConnectionPool(
            1, 20,
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
    
    def get_connection(self):
        return self.pool.getconn()
    
    def return_connection(self, conn):
        self.pool.putconn(conn)
```

## Deployment Architecture

### Docker Compose Stack

The medallion architecture is deployed using Docker Compose for local development and testing.

**Services**:
- **Kafka**: 3-node cluster
- **PostgreSQL**: Metadata store
- **MinIO**: S3-compatible storage
- **Spark Master**: Spark cluster master
- **Spark Worker**: Spark cluster workers (scalable)
- **Prometheus**: Metrics collection
- **Grafana**: Dashboards
- **Elasticsearch**: Log storage
- **Kibana**: Log visualization

**Docker Compose Configuration**:
```yaml
version: '3.8'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1

  postgres:
    image: postgres:15-alpine
    environment:
      POSTGRES_DB: medallion_metadata
      POSTGRES_USER: medallion_user
      POSTGRES_PASSWORD: [secure_password]
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data

  minio:
    image: minio/minio:latest
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: [secure_password]
    volumes:
      - minio_data:/data
    command: server /data --console-address ":9001"

  spark-master:
    image: bitnami/spark:3.5.0
    ports:
      - "8080:8080"
      - "7077:7077"
    environment:
      SPARK_MODE: master
      SPARK_RPC_AUTHENTICATION_ENABLED: "no"

  prometheus:
    image: prom/prometheus:latest
    ports:
      - "9090:9090"
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
      - prometheus_data:/prometheus

  grafana:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    environment:
      GF_SECURITY_ADMIN_PASSWORD: admin
    volumes:
      - grafana_data:/var/lib/grafana

volumes:
  postgres_data:
  minio_data:
  prometheus_data:
  grafana_data:
```

## Security Considerations

### Data Encryption

- **At Rest**: AES-256 encryption for S3 objects
- **In Transit**: TLS 1.2+ for all network communication
- **Credentials**: Stored in secure vaults (HashiCorp Vault or AWS Secrets Manager)

### Access Control

- **Authentication**: OAuth 2.0 or LDAP integration
- **Authorization**: Role-based access control (RBAC)
- **Audit Logging**: All access attempts logged to PostgreSQL

### Network Security

- **Firewall Rules**: Restrict access to internal networks only
- **VPC Isolation**: Deploy within private VPCs
- **Service Mesh**: Optional Istio for advanced traffic management

## Performance Optimization

### Partitioning Strategy

- **Bronze Layer**: Partitioned by producer and date
- **Silver Layer**: Partitioned by entity type and date
- **Gold Layer**: Partitioned by domain and date

### Caching Strategy

- **Spark Caching**: Cache frequently accessed Silver/Gold datasets
- **Query Result Caching**: Cache Gold layer query results in Redis

### Compression

- **Parquet Snappy**: Balanced compression ratio and speed
- **Kafka Compression**: Snappy for message compression

## Monitoring and Alerting

### Key Metrics

- **Throughput**: Records per second by layer
- **Latency**: End-to-end processing time
- **Error Rate**: Failed records percentage
- **Queue Depth**: Pending records in queues
- **Storage Usage**: Data lake storage consumption

### Alert Thresholds

- **High Error Rate**: > 1% errors in 5 minutes
- **Low Throughput**: < 100 records/second for 10 minutes
- **High Latency**: p95 latency > 5 minutes
- **Storage Alert**: > 80% capacity used

## Disaster Recovery

### Backup Strategy

- **Daily Backups**: Full backup of PostgreSQL metadata
- **S3 Versioning**: Enable versioning on all S3 buckets
- **Checkpoint Retention**: Keep 7 days of Spark checkpoints

### Recovery Procedures

1. **Data Loss**: Restore from S3 versioning or backups
2. **Metadata Loss**: Restore PostgreSQL from backup
3. **Component Failure**: Automatic restart via orchestrator
4. **Cascading Failure**: Manual intervention with documented runbooks

## Conclusion

The Medallion Architecture provides a scalable, maintainable, and cost-effective data lake solution for masterflow. By leveraging open-source technologies and following data engineering best practices, this architecture enables progressive data refinement from raw ingestion through business-ready analytics while maintaining data quality, lineage, and security throughout the pipeline.
