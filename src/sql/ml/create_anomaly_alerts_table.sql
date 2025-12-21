CREATE TABLE IF NOT EXISTS ANALYTICS_DB.ML_RESULTS.ANOMALY_ALERTS (
    alert_id VARCHAR DEFAULT UUID_STRING(),
    property_id VARCHAR,
    anomaly_type VARCHAR,
    anomaly_score FLOAT,
    expected_value FLOAT,
    actual_value FLOAT,
    description TEXT,
    severity VARCHAR,
    detected_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    is_resolved BOOLEAN DEFAULT FALSE,
    resolved_at TIMESTAMP_NTZ,
    PRIMARY KEY (alert_id)
)
