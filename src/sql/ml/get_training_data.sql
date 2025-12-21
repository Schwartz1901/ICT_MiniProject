SELECT
    id,
    price,
    price_per_sqm,
    area,
    bedrooms,
    bathrooms,
    rooms_total,
    city,
    district,
    latitude,
    longitude,
    price_category,
    area_category,
    is_high_value
FROM ANALYTICS_DB.DWH.GOLD_ML_FEATURES
WHERE price IS NOT NULL
AND area IS NOT NULL
AND price > 0
AND area > 0
