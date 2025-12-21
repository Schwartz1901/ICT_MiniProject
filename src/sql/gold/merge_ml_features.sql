MERGE INTO ANALYTICS_DB.DWH.GOLD_ML_FEATURES t
USING (
    SELECT
        id,
        price,
        CASE WHEN area > 0 THEN price / area ELSE NULL END AS price_per_sqm,
        area,
        bedrooms,
        bathrooms,
        COALESCE(bedrooms, 0) + COALESCE(bathrooms, 0) AS rooms_total,
        city,
        district,
        latitude,
        longitude,
        price > {p75} AS is_high_value,
        CASE
            WHEN price <= {p25} THEN 'Budget'
            WHEN price <= {p50} THEN 'Standard'
            WHEN price <= {p75} THEN 'Premium'
            ELSE 'Luxury'
        END AS price_category,
        CASE
            WHEN area < 50 THEN 'Small'
            WHEN area < 100 THEN 'Medium'
            WHEN area < 200 THEN 'Large'
            ELSE 'Very Large'
        END AS area_category
    FROM STAGING_DB.CLEANED.SILVER_REAL_ESTATE
    WHERE data_quality_score >= {min_quality_score}
    AND price > 0
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY data_quality_score DESC) = 1
) s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET
    price = s.price,
    price_per_sqm = s.price_per_sqm,
    area = s.area,
    bedrooms = s.bedrooms,
    bathrooms = s.bathrooms,
    rooms_total = s.rooms_total,
    city = s.city,
    district = s.district,
    latitude = s.latitude,
    longitude = s.longitude,
    is_high_value = s.is_high_value,
    price_category = s.price_category,
    area_category = s.area_category,
    processed_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    id, price, price_per_sqm, area, bedrooms, bathrooms,
    rooms_total, city, district, latitude, longitude,
    is_high_value, price_category, area_category
) VALUES (
    s.id, s.price, s.price_per_sqm, s.area, s.bedrooms, s.bathrooms,
    s.rooms_total, s.city, s.district, s.latitude, s.longitude,
    s.is_high_value, s.price_category, s.area_category
)
