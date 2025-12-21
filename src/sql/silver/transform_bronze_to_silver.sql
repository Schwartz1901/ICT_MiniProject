INSERT INTO STAGING_DB.CLEANED.SILVER_REAL_ESTATE (
    id,
    property_type,
    price,
    area,
    bedrooms,
    bathrooms,
    location,
    city,
    district,
    latitude,
    longitude,
    description,
    posted_date,
    data_quality_score
)
SELECT
    id,
    -- Clean and extract fields from raw JSON
    TRIM(raw_data:property_type::VARCHAR),
    TRY_CAST(raw_data:price AS FLOAT),
    TRY_CAST(raw_data:area AS FLOAT),
    TRY_CAST(raw_data:bedrooms AS INT),
    TRY_CAST(raw_data:bathrooms AS INT),
    TRIM(raw_data:location::VARCHAR),
    TRIM(raw_data:city::VARCHAR),
    TRIM(raw_data:district::VARCHAR),
    TRY_CAST(raw_data:latitude AS FLOAT),
    TRY_CAST(raw_data:longitude AS FLOAT),
    TRIM(raw_data:description::VARCHAR),
    TRY_CAST(raw_data:posted_date AS DATE),
    -- Calculate data quality score (0.0 - 1.0)
    (
        CASE WHEN raw_data:price IS NOT NULL AND TRY_CAST(raw_data:price AS FLOAT) > 0 THEN 0.2 ELSE 0 END +
        CASE WHEN raw_data:area IS NOT NULL AND TRY_CAST(raw_data:area AS FLOAT) > 0 THEN 0.2 ELSE 0 END +
        CASE WHEN raw_data:location IS NOT NULL AND LENGTH(raw_data:location::VARCHAR) > 0 THEN 0.2 ELSE 0 END +
        CASE WHEN raw_data:bedrooms IS NOT NULL THEN 0.2 ELSE 0 END +
        CASE WHEN raw_data:city IS NOT NULL AND LENGTH(raw_data:city::VARCHAR) > 0 THEN 0.2 ELSE 0 END
    ) AS data_quality_score
FROM RAW_DB.LANDING.BRONZE_REAL_ESTATE
WHERE id NOT IN (SELECT id FROM STAGING_DB.CLEANED.SILVER_REAL_ESTATE)
