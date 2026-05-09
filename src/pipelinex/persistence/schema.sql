-- PipelineX PostgreSQL schema.
-- Loaded automatically by docker-compose into the pipelinex_postgres container
-- (see docker-compose.yml, mounted at /docker-entrypoint-initdb.d/).

-- Logs flow through here as the primary data store.
CREATE TABLE IF NOT EXISTS log_records (
    id UUID PRIMARY KEY,
    pipeline_run_id UUID,
    timestamp TIMESTAMPTZ,
    source VARCHAR(255) NOT NULL DEFAULT '',
    severity VARCHAR(20) NOT NULL DEFAULT 'INFO',
    message TEXT NOT NULL DEFAULT '',
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    enrichment JSONB NOT NULL DEFAULT '{}'::jsonb,
    stage_history TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[]
);

CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON log_records (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_logs_source ON log_records (source);
CREATE INDEX IF NOT EXISTS idx_logs_severity ON log_records (severity);
CREATE INDEX IF NOT EXISTS idx_logs_run_id ON log_records (pipeline_run_id);
CREATE INDEX IF NOT EXISTS idx_logs_payload ON log_records USING GIN (raw_payload);
CREATE INDEX IF NOT EXISTS idx_logs_enrichment ON log_records USING GIN (enrichment);

-- Block traces — closed sequences of events for a single HDFS block.
-- Anomaly detection on HDFS happens here, not on individual records.
CREATE TABLE IF NOT EXISTS block_traces (
    block_id VARCHAR(64) PRIMARY KEY,
    pipeline_run_id UUID NOT NULL,
    first_timestamp TIMESTAMPTZ,
    last_timestamp TIMESTAMPTZ,
    record_count INT NOT NULL,
    event_sequence TEXT[] NOT NULL,
    closed_reason VARCHAR(20) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_traces_run_id ON block_traces (pipeline_run_id);
CREATE INDEX IF NOT EXISTS idx_traces_last_ts ON block_traces (last_timestamp DESC);

-- Anomaly events — detected anomalies from any detector.
CREATE TABLE IF NOT EXISTS anomaly_events (
    id UUID PRIMARY KEY,
    log_record_id UUID NOT NULL,
    detector_name VARCHAR(100) NOT NULL,
    severity_score DOUBLE PRECISION NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_anomalies_detected_at ON anomaly_events (detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_anomalies_detector ON anomaly_events (detector_name);
