SELECT
    COUNT(*) as total_records,
    COUNT(DISTINCT city) as unique_cities,
    AVG(price) as avg_price,
    STDDEV(price) as std_price,
    AVG(area) as avg_area,
    STDDEV(area) as std_area,
    AVG(price_per_sqm) as avg_price_per_sqm,
    COUNT(CASE WHEN price_category = 'Budget' THEN 1 END) as budget_count,
    COUNT(CASE WHEN price_category = 'Standard' THEN 1 END) as standard_count,
    COUNT(CASE WHEN price_category = 'Premium' THEN 1 END) as premium_count,
    COUNT(CASE WHEN price_category = 'Luxury' THEN 1 END) as luxury_count
FROM ANALYTICS_DB.DWH.GOLD_ML_FEATURES
WHERE price IS NOT NULL AND area IS NOT NULL
