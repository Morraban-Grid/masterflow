-- Medallion Architecture PostgreSQL Initialization Script
-- This script creates the necessary schemas and tables for the medallion architecture

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
    status VARCHAR(50) DEFAULT 'active' CHECK (status IN ('active', 'deprecated', 'archived')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deprecated_at TIMESTAMP,
    created_by VARCHAR(255),
    UNIQUE(name, version),
    INDEX idx_schema_name (name),
    INDEX idx_schema_status (status),
    INDEX idx_schema_created_at (created_at)
);

CREATE TABLE IF NOT EXISTS medallion.schema_evolution (
    id SERIAL PRIMARY KEY,
    from_version INT NOT NULL,
    to_version INT NOT NULL,
    schema_name VARCHAR(255) NOT NULL,
    is_backward_compatible BOOLEAN DEFAULT TRUE,
    breaking_changes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(255),
    FOREIGN KEY (schema_name) REFERENCES medallion.schemas(name),
    INDEX idx_evolution_schema (schema_name),
    INDEX idx_evolution_created_at (created_at)
);

CREATE TABLE IF NOT EXISTS medallion.data_layers (
    id SERIAL PRIMARY KEY,
    layer_name VARCHAR(50) NOT NULL UNIQUE CHECK (layer_name IN ('bronze', 'silver', 'gold')),
    description TEXT,
    retention_days INT DEFAULT 90,
    encryption_enabled BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_record_id (record_id),
    INDEX idx_created_at (created_at),
    INDEX idx_source_layer (source_layer),
    INDEX idx_destination_layer (destination_layer)
);

CREATE TABLE IF NOT EXISTS lineage.lineage_transformations (
    id SERIAL PRIMARY KEY,
    lineage_id INT NOT NULL,
    transformation_type VARCHAR(100),
    transformation_name VARCHAR(255),
    parameters JSONB,
    execution_time_ms INT,
    status VARCHAR(50) CHECK (status IN ('success', 'failed', 'skipped')),
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (lineage_id) REFERENCES lineage.lineage_records(id) ON DELETE CASCADE,
    INDEX idx_lineage_id (lineage_id),
    INDEX idx_transformation_type (transformation_type),
    INDEX idx_created_at (created_at)
);

CREATE TABLE IF NOT EXISTS lineage.quality_check_results (
    id SERIAL PRIMARY KEY,
    lineage_id INT NOT NULL,
    check_type VARCHAR(100),
    check_name VARCHAR(255),
    passed BOOLEAN,
    affected_records INT,
    message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (lineage_id) REFERENCES lineage.lineage_records(id) ON DELETE CASCADE,
    INDEX idx_lineage_id (lineage_id),
    INDEX idx_check_type (check_type),
    INDEX idx_passed (passed),
    INDEX idx_created_at (created_at)
);

-- ============================================================================
-- ACCESS_CONTROL SCHEMA - Access Control and Security
-- ============================================================================

CREATE TABLE IF NOT EXISTS access_control.access_policies (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL,
    dataset_path VARCHAR(1024) NOT NULL,
    access_level VARCHAR(50) NOT NULL CHECK (access_level IN ('read', 'write', 'admin')),
    column_restrictions JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP,
    created_by VARCHAR(255),
    UNIQUE(user_id, dataset_path),
    INDEX idx_user_id (user_id),
    INDEX idx_dataset_path (dataset_path),
    INDEX idx_expires_at (expires_at)
);

CREATE TABLE IF NOT EXISTS access_control.access_audit_log (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL,
    dataset_path VARCHAR(1024) NOT NULL,
    access_type VARCHAR(50),
    allowed BOOLEAN,
    reason VARCHAR(255),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_user_timestamp (user_id, timestamp),
    INDEX idx_dataset_timestamp (dataset_path, timestamp),
    INDEX idx_allowed (allowed),
    INDEX idx_timestamp (timestamp)
);

-- ============================================================================
-- MONITORING SCHEMA - Monitoring and Metrics
-- ============================================================================

CREATE TABLE IF NOT EXISTS monitoring.pipeline_metrics (
    id SERIAL PRIMARY KEY,
    layer VARCHAR(50) NOT NULL,
    metric_name VARCHAR(255) NOT NULL,
    metric_value NUMERIC,
    metric_unit VARCHAR(50),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_layer_timestamp (layer, timestamp),
    INDEX idx_metric_name (metric_name),
    INDEX idx_timestamp (timestamp)
);

CREATE TABLE IF NOT EXISTS monitoring.pipeline_alerts (
    id SERIAL PRIMARY KEY,
    alert_name VARCHAR(255) NOT NULL,
    severity VARCHAR(50) CHECK (severity IN ('info', 'warning', 'critical')),
    message TEXT,
    affected_component VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    resolved_at TIMESTAMP,
    INDEX idx_severity (severity),
    INDEX idx_created_at (created_at),
    INDEX idx_resolved_at (resolved_at)
);

-- ============================================================================
-- INDEXES FOR PERFORMANCE
-- ============================================================================

-- Create composite indexes for common queries
CREATE INDEX IF NOT EXISTS idx_lineage_source_dest ON lineage.lineage_records(source_layer, destination_layer, created_at);
CREATE INDEX IF NOT EXISTS idx_access_policy_user_level ON access_control.access_policies(user_id, access_level);
CREATE INDEX IF NOT EXISTS idx_audit_log_user_action ON access_control.access_audit_log(user_id, access_type, timestamp);

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

-- ============================================================================
-- COMMENTS FOR DOCUMENTATION
-- ============================================================================

COMMENT ON SCHEMA medallion IS 'Core medallion architecture metadata and schema registry';
COMMENT ON SCHEMA lineage IS 'Data lineage and provenance tracking';
COMMENT ON SCHEMA access_control IS 'Access control policies and audit logging';
COMMENT ON SCHEMA monitoring IS 'Pipeline monitoring and alerting';

COMMENT ON TABLE medallion.schemas IS 'Schema definitions and versioning for all medallion layers';
COMMENT ON TABLE lineage.lineage_records IS 'Complete data lineage tracking from source to destination';
COMMENT ON TABLE access_control.access_policies IS 'User access control policies for datasets';
COMMENT ON TABLE access_control.access_audit_log IS 'Audit trail of all access control decisions';
COMMENT ON TABLE monitoring.pipeline_metrics IS 'Pipeline performance metrics and statistics';
COMMENT ON TABLE monitoring.pipeline_alerts IS 'Pipeline alerts and notifications';
