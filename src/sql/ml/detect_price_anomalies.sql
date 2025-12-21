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
    'PRICE_OUTLIER',
    ABS(f.price - city_stats.avg_price) / NULLIF(city_stats.std_price, 0) AS anomaly_score,
    city_stats.avg_price AS expected_value,
    f.price AS actual_value,
    CONCAT('Price deviates ', ROUND(ABS(f.price - city_stats.avg_price) / NULLIF(city_stats.std_price, 0), 1), ' std deviations from city average') AS description,
    CASE
        WHEN ABS(f.price - city_stats.avg_price) / NULLIF(city_stats.std_price, 0) > 4 THEN 'HIGH'
        WHEN ABS(f.price - city_stats.avg_price) / NULLIF(city_stats.std_price, 0) > 3 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS severity
FROM ANALYTICS_DB.DWH.GOLD_ML_FEATURES f
JOIN (
    SELECT
        city,
        AVG(price) AS avg_price,
        STDDEV(price) AS std_price
    FROM ANALYTICS_DB.DWH.GOLD_ML_FEATURES
    GROUP BY city
    HAVING COUNT(*) >= 5
) city_stats ON f.city = city_stats.city
WHERE ABS(f.price - city_stats.avg_price) / NULLIF(city_stats.std_price, 0) > 3
AND f.id NOT IN (
    SELECT property_id FROM ANALYTICS_DB.ML_RESULTS.ANOMALY_ALERTS
    WHERE anomaly_type = 'PRICE_OUTLIER'
)
