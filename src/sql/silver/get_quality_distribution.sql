SELECT
    CASE
        WHEN data_quality_score >= 0.8 THEN 'Excellent (0.8-1.0)'
        WHEN data_quality_score >= 0.6 THEN 'Good (0.6-0.8)'
        WHEN data_quality_score >= 0.4 THEN 'Fair (0.4-0.6)'
        ELSE 'Poor (0-0.4)'
    END as quality_bucket,
    COUNT(*) as count
FROM STAGING_DB.CLEANED.SILVER_REAL_ESTATE
GROUP BY quality_bucket
ORDER BY quality_bucket DESC
