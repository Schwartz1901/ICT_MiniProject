CREATE TABLE IF NOT EXISTS SILVER_REAL_ESTATE (
    id VARCHAR PRIMARY KEY,
    property_type VARCHAR,
    price FLOAT,
    area FLOAT,
    bedrooms INT,
    bathrooms INT,
    location VARCHAR,
    city VARCHAR,
    district VARCHAR,
    latitude FLOAT,
    longitude FLOAT,
    description TEXT,
    posted_date DATE,
    processed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    data_quality_score FLOAT
)
COMMENT = 'Cleaned and structured real estate data'
