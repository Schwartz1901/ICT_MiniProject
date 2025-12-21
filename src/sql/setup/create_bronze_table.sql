CREATE TABLE IF NOT EXISTS BRONZE_REAL_ESTATE (
    id VARCHAR,
    raw_data VARIANT,
    ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source VARCHAR DEFAULT 'mongodb',
    kafka_partition INT,
    kafka_offset INT
)
COMMENT = 'Raw real estate data from MongoDB via Kafka'
