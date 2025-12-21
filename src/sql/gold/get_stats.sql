SELECT
    COUNT(*) as total_features,
    COUNT(DISTINCT city) as unique_cities,
    COUNT(CASE WHEN is_high_value THEN 1 END) as high_value_count,
    AVG(price) as avg_price,
    AVG(price_per_sqm) as avg_price_per_sqm
FROM ANALYTICS_DB.DWH.GOLD_ML_FEATURES
