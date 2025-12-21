INSERT INTO ANALYTICS_DB.ML_RESULTS.ANOMALY_ALERTS (
    property_id,
    anomaly_type,
    anomaly_score,
    expected_value,
    actual_value,
    description,
    severity
)
SELECT
    f.id,
    'PRICE_PER_SQM_OUTLIER',
    ABS(f.price_per_sqm - city_stats.avg_price_per_sqm) / NULLIF(city_stats.std_price_per_sqm, 0) AS anomaly_score,
    city_stats.avg_price_per_sqm AS expected_value,
    f.price_per_sqm AS actual_value,
    CONCAT('Price per sqm deviates ', ROUND(ABS(f.price_per_sqm - city_stats.avg_price_per_sqm) / NULLIF(city_stats.std_price_per_sqm, 0), 1), ' std deviations from city average') AS description,
    CASE
        WHEN ABS(f.price_per_sqm - city_stats.avg_price_per_sqm) / NULLIF(city_stats.std_price_per_sqm, 0) > 4 THEN 'HIGH'
        WHEN ABS(f.price_per_sqm - city_stats.avg_price_per_sqm) / NULLIF(city_stats.std_price_per_sqm, 0) > 3 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS severity
FROM ANALYTICS_DB.DWH.GOLD_ML_FEATURES f
JOIN (
    SELECT
        city,
        AVG(price_per_sqm) AS avg_price_per_sqm,
        STDDEV(price_per_sqm) AS std_price_per_sqm
    FROM ANALYTICS_DB.DWH.GOLD_ML_FEATURES
    WHERE price_per_sqm IS NOT NULL
    GROUP BY city
    HAVING COUNT(*) >= 5
) city_stats ON f.city = city_stats.city
WHERE f.price_per_sqm IS NOT NULL
AND ABS(f.price_per_sqm - city_stats.avg_price_per_sqm) / NULLIF(city_stats.std_price_per_sqm, 0) > 3
AND f.id NOT IN (
    SELECT property_id FROM ANALYTICS_DB.ML_RESULTS.ANOMALY_ALERTS
    WHERE anomaly_type = 'PRICE_PER_SQM_OUTLIER'
)
