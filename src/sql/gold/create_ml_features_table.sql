CREATE TABLE IF NOT EXISTS ANALYTICS_DB.DWH.GOLD_ML_FEATURES (
    id VARCHAR PRIMARY KEY,
    -- Numeric features
    price FLOAT,
    price_per_sqm FLOAT,
    area FLOAT,
    bedrooms INT,
    bathrooms INT,
    rooms_total INT,
    -- Location features
    city VARCHAR,
    district VARCHAR,
    latitude FLOAT,
    longitude FLOAT,
    -- Derived features
    is_high_value BOOLEAN,
    price_category VARCHAR,
    area_category VARCHAR,
    -- Metadata
    processed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
