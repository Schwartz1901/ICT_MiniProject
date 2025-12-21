SELECT
    COUNT(*) as total_records,
    COUNT(DISTINCT id) as unique_ids,
    MIN(ingested_at) as first_ingestion,
    MAX(ingested_at) as last_ingestion
FROM RAW_DB.LANDING.BRONZE_REAL_ESTATE
