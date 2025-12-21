CREATE OR REPLACE VIEW V_ACTIVE_ANOMALIES AS
SELECT
    alert_id,
    property_id,
    anomaly_type,
    anomaly_score,
    expected_value,
    actual_value,
    description,
    severity,
    detected_at
FROM ANALYTICS_DB.ML_RESULTS.ANOMALY_ALERTS
WHERE is_resolved = FALSE
ORDER BY severity DESC, detected_at DESC
