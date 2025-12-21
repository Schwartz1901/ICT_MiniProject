MERGE INTO ANALYTICS_DB.DWH.GOLD_MARKET_ANALYTICS t
USING (
    SELECT
        city,
        district,
        property_type,
        AVG(price) AS avg_price,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) AS median_price,
        MIN(price) AS min_price,
        MAX(price) AS max_price,
        AVG(CASE WHEN area > 0 THEN price / area END) AS avg_price_per_sqm,
        COUNT(*) AS total_listings,
        AVG(area) AS avg_area,
        AVG(bedrooms) AS avg_bedrooms
    FROM STAGING_DB.CLEANED.SILVER_REAL_ESTATE
    WHERE data_quality_score >= {min_quality_score}
    AND price > 0
    GROUP BY city, district, property_type
) s
ON t.city = s.city AND COALESCE(t.district, '') = COALESCE(s.district, '') AND COALESCE(t.property_type, '') = COALESCE(s.property_type, '')
WHEN MATCHED THEN UPDATE SET
    avg_price = s.avg_price,
    median_price = s.median_price,
    min_price = s.min_price,
    max_price = s.max_price,
    avg_price_per_sqm = s.avg_price_per_sqm,
    total_listings = s.total_listings,
    avg_area = s.avg_area,
    avg_bedrooms = s.avg_bedrooms,
    updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    city, district, property_type, avg_price, median_price,
    min_price, max_price, avg_price_per_sqm, total_listings, avg_area, avg_bedrooms
) VALUES (
    s.city, s.district, s.property_type, s.avg_price, s.median_price,
    s.min_price, s.max_price, s.avg_price_per_sqm, s.total_listings, s.avg_area, s.avg_bedrooms
)
