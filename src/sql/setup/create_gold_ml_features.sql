CREATE TABLE IF NOT EXISTS GOLD_ML_FEATURES (
    id VARCHAR PRIMARY KEY,
    price FLOAT,
    price_per_sqm FLOAT,
    area FLOAT,
    bedrooms INT,
    bathrooms INT,
    rooms_total INT,
    city VARCHAR,
    district VARCHAR,
    latitude FLOAT,
    longitude FLOAT,
    is_high_value BOOLEAN,
    price_category VARCHAR,
    area_category VARCHAR,
    processed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Feature-engineered data for ML models'
