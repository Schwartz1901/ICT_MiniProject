CREATE TABLE IF NOT EXISTS GOLD_MARKET_ANALYTICS (
    city VARCHAR,
    district VARCHAR,
    property_type VARCHAR,
    avg_price FLOAT,
    median_price FLOAT,
    min_price FLOAT,
    max_price FLOAT,
    avg_price_per_sqm FLOAT,
    total_listings INT,
    avg_area FLOAT,
    avg_bedrooms FLOAT,
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (city, district, property_type)
)
COMMENT = 'Aggregated market analytics by location'
