CREATE TABLE IF NOT EXISTS RAW_DB.LANDING.BRONZE_REAL_ESTATE (
    id VARCHAR,
    raw_data VARCHAR(16777216),
    ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source VARCHAR DEFAULT 'mongodb',
    kafka_partition INT,
    kafka_offset INT
)
