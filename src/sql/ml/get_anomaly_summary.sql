SELECT
    anomaly_type,
    severity,
    COUNT(*) as count,
    AVG(anomaly_score) as avg_score
FROM ANALYTICS_DB.ML_RESULTS.ANOMALY_ALERTS
WHERE is_resolved = FALSE
GROUP BY anomaly_type, severity
ORDER BY anomaly_type, severity
