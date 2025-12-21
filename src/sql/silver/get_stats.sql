SELECT
    COUNT(*) as total_records,
    AVG(data_quality_score) as avg_quality_score,
    COUNT(CASE WHEN data_quality_score >= 0.6 THEN 1 END) as high_quality_records,
    COUNT(DISTINCT city) as unique_cities,
    AVG(price) as avg_price,
    AVG(area) as avg_area
FROM STAGING_DB.CLEANED.SILVER_REAL_ESTATE
