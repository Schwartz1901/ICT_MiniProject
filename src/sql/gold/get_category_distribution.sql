SELECT
    price_category,
    COUNT(*) as count
FROM ANALYTICS_DB.DWH.GOLD_ML_FEATURES
GROUP BY price_category
ORDER BY
    CASE price_category
        WHEN 'Budget' THEN 1
        WHEN 'Standard' THEN 2
        WHEN 'Premium' THEN 3
        WHEN 'Luxury' THEN 4
    END
