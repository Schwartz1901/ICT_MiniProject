MERGE INTO ANALYTICS_DB.DWH.GOLD_PRICE_TRENDS t
USING (
    SELECT
        COALESCE(city, 'Unknown') AS city,
        TO_VARCHAR(posted_date, 'YYYY-MM') AS year_month,
        AVG(price) AS avg_price,
        AVG(CASE WHEN area > 0 THEN price / area END) AS avg_price_per_sqm,
        COUNT(*) AS listing_count
    FROM STAGING_DB.CLEANED.SILVER_REAL_ESTATE
    WHERE data_quality_score >= {min_quality_score}
    AND price > 0
    AND posted_date IS NOT NULL
    GROUP BY COALESCE(city, 'Unknown'), TO_VARCHAR(posted_date, 'YYYY-MM')
) s
ON t.city = s.city AND t.year_month = s.year_month
WHEN MATCHED THEN UPDATE SET
    avg_price = s.avg_price,
    avg_price_per_sqm = s.avg_price_per_sqm,
    listing_count = s.listing_count,
    updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    city, year_month, avg_price, avg_price_per_sqm, listing_count
) VALUES (
    s.city, s.year_month, s.avg_price, s.avg_price_per_sqm, s.listing_count
)
