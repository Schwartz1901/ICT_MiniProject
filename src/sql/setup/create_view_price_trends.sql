CREATE OR REPLACE VIEW V_PRICE_TRENDS AS
SELECT
    city,
    year_month,
    avg_price,
    avg_price_per_sqm,
    listing_count,
    updated_at
FROM ANALYTICS_DB.DWH.GOLD_PRICE_TRENDS
ORDER BY city, year_month
