-- Medallion Architecture PostgreSQL Initialization Script
-- Simplified version for MVP - focuses on core tables

-- Create schemas
CREATE SCHEMA IF NOT EXISTS medallion;
CREATE SCHEMA IF NOT EXISTS lineage;
CREATE SCHEMA IF NOT EXISTS access_control;
CREATE SCHEMA IF NOT EXISTS monitoring;

-- Set search path
SET search_path TO medallion, lineage, access_control, monitoring, public;

-- ============================================================================
-- MEDALLION SCHEMA - Schema Registry and Core Metadata
-- ============================================================================

CREATE TABLE IF NOT EXISTS medallion.schemas (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    version INT NOT NULL,
    definition JSONB NOT NULL,
    status VARCHAR(50) DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(255),
    UNIQUE(name, version)
);

CREATE INDEX IF NOT EXISTS idx_schema_name ON medallion.schemas(name);
CREATE INDEX IF NOT EXISTS idx_schema_status ON medallion.schemas(status);
CREATE INDEX IF NOT EXISTS idx_schema_created_at ON medallion.schemas(created_at);

CREATE TABLE IF NOT EXISTS medallion.data_layers (
    id SERIAL PRIMARY KEY,
    layer_name VARCHAR(50) NOT NULL UNIQUE,
    description TEXT,
    retention_days INT DEFAULT 90,
    encryption_enabled BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert default layers
INSERT INTO medallion.data_layers (layer_name, description, retention_days) VALUES
    ('bronze', 'Raw, immutable data layer', 90),
    ('silver', 'Cleaned and standardized data layer', 365),
    ('gold', 'Business-ready aggregated data layer', NULL)
ON CONFLICT (layer_name) DO NOTHING;

-- ============================================================================
-- LINEAGE SCHEMA - Data Lineage and Provenance Tracking
-- ============================================================================

CREATE TABLE IF NOT EXISTS lineage.lineage_records (
    id SERIAL PRIMARY KEY,
    record_id UUID NOT NULL,
    source_layer VARCHAR(50) NOT NULL,
    source_producer VARCHAR(255),
    source_topic VARCHAR(255),
    destination_layer VARCHAR(50) NOT NULL,
    destination_path VARCHAR(1024),
    transformations JSONB,
    quality_checks JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_record_id ON lineage.lineage_records(record_id);
CREATE INDEX IF NOT EXISTS idx_created_at ON lineage.lineage_records(created_at);
CREATE INDEX IF NOT EXISTS idx_source_layer ON lineage.lineage_records(source_layer);
CREATE INDEX IF NOT EXISTS idx_destination_layer ON lineage.lineage_records(destination_layer);

CREATE TABLE IF NOT EXISTS lineage.lineage_transformations (
    id SERIAL PRIMARY KEY,
    lineage_id INT NOT NULL,
    transformation_type VARCHAR(100),
    transformation_name VARCHAR(255),
    parameters JSONB,
    execution_time_ms INT,
    status VARCHAR(50),
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_lineage_id ON lineage.lineage_transformations(lineage_id);
CREATE INDEX IF NOT EXISTS idx_transformation_type ON lineage.lineage_transformations(transformation_type);
CREATE INDEX IF NOT EXISTS idx_transformation_created_at ON lineage.lineage_transformations(created_at);

CREATE TABLE IF NOT EXISTS lineage.quality_check_results (
    id SERIAL PRIMARY KEY,
    lineage_id INT NOT NULL,
    check_type VARCHAR(100),
    check_name VARCHAR(255),
    passed BOOLEAN,
    affected_records INT,
    message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_quality_lineage_id ON lineage.quality_check_results(lineage_id);
CREATE INDEX IF NOT EXISTS idx_quality_check_type ON lineage.quality_check_results(check_type);
CREATE INDEX IF NOT EXISTS idx_quality_passed ON lineage.quality_check_results(passed);
CREATE INDEX IF NOT EXISTS idx_quality_created_at ON lineage.quality_check_results(created_at);

-- ============================================================================
-- ACCESS_CONTROL SCHEMA - Access Control and Security
-- ============================================================================

CREATE TABLE IF NOT EXISTS access_control.access_policies (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL,
    dataset_path VARCHAR(1024) NOT NULL,
    access_level VARCHAR(50) NOT NULL,
    column_restrictions JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP,
    created_by VARCHAR(255),
    UNIQUE(user_id, dataset_path)
);

CREATE INDEX IF NOT EXISTS idx_access_user_id ON access_control.access_policies(user_id);
CREATE INDEX IF NOT EXISTS idx_access_dataset_path ON access_control.access_policies(dataset_path);
CREATE INDEX IF NOT EXISTS idx_access_expires_at ON access_control.access_policies(expires_at);

CREATE TABLE IF NOT EXISTS access_control.access_audit_log (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL,
    dataset_path VARCHAR(1024) NOT NULL,
    access_type VARCHAR(50),
    allowed BOOLEAN,
    reason VARCHAR(255),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_audit_user_timestamp ON access_control.access_audit_log(user_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_audit_dataset_timestamp ON access_control.access_audit_log(dataset_path, timestamp);
CREATE INDEX IF NOT EXISTS idx_audit_allowed ON access_control.access_audit_log(allowed);
CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON access_control.access_audit_log(timestamp);

-- ============================================================================
-- MONITORING SCHEMA - Monitoring and Metrics
-- ============================================================================

CREATE TABLE IF NOT EXISTS monitoring.pipeline_metrics (
    id SERIAL PRIMARY KEY,
    layer VARCHAR(50) NOT NULL,
    metric_name VARCHAR(255) NOT NULL,
    metric_value NUMERIC,
    metric_unit VARCHAR(50),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_metrics_layer_timestamp ON monitoring.pipeline_metrics(layer, timestamp);
CREATE INDEX IF NOT EXISTS idx_metrics_metric_name ON monitoring.pipeline_metrics(metric_name);
CREATE INDEX IF NOT EXISTS idx_metrics_timestamp ON monitoring.pipeline_metrics(timestamp);

CREATE TABLE IF NOT EXISTS monitoring.pipeline_alerts (
    id SERIAL PRIMARY KEY,
    alert_name VARCHAR(255) NOT NULL,
    severity VARCHAR(50),
    message TEXT,
    affected_component VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    resolved_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_alerts_severity ON monitoring.pipeline_alerts(severity);
CREATE INDEX IF NOT EXISTS idx_alerts_created_at ON monitoring.pipeline_alerts(created_at);
CREATE INDEX IF NOT EXISTS idx_alerts_resolved_at ON monitoring.pipeline_alerts(resolved_at);

-- ============================================================================
-- GRANT PERMISSIONS
-- ============================================================================

-- Grant permissions to medallion_user
GRANT USAGE ON SCHEMA medallion, lineage, access_control, monitoring TO medallion_user;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA medallion TO medallion_user;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA lineage TO medallion_user;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA access_control TO medallion_user;
GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA monitoring TO medallion_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA medallion, lineage, access_control, monitoring TO medallion_user;
