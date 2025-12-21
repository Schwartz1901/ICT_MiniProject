CREATE TABLE IF NOT EXISTS GOLD_PRICE_TRENDS (
    city VARCHAR,
    year_month VARCHAR,
    avg_price FLOAT,
    avg_price_per_sqm FLOAT,
    listing_count INT,
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (city, year_month)
)
COMMENT = 'Time series price trends by city'
