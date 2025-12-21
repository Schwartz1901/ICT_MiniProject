CREATE OR REPLACE VIEW V_MARKET_SUMMARY AS
SELECT
    city,
    district,
    property_type,
    avg_price,
    median_price,
    min_price,
    max_price,
    avg_price_per_sqm,
    total_listings,
    avg_area,
    avg_bedrooms,
    updated_at
FROM ANALYTICS_DB.DWH.GOLD_MARKET_ANALYTICS
