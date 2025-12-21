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
WITH parsed AS (
    SELECT
        id,
        TRY_PARSE_JSON(raw_data) AS data
    FROM RAW_DB.LANDING.BRONZE_REAL_ESTATE
    WHERE id NOT IN (SELECT id FROM STAGING_DB.CLEANED.SILVER_REAL_ESTATE)
)
SELECT
    id,
    -- Extract property type from link (ban-can-ho, ban-nha, etc.)
    CASE
        WHEN data:"Link"::VARCHAR LIKE '%ban-can-ho%' THEN 'Căn hộ'
        WHEN data:"Link"::VARCHAR LIKE '%ban-nha%' THEN 'Nhà'
        WHEN data:"Link"::VARCHAR LIKE '%ban-dat%' THEN 'Đất'
        WHEN data:"Link"::VARCHAR LIKE '%ban-biet-thu%' THEN 'Biệt thự'
        ELSE 'Khác'
    END AS property_type,
    -- Parse price (extract numeric value, handle "tỷ" = billion, "triệu" = million)
    CASE
        WHEN data:"Mức giá"::VARCHAR LIKE '%tỷ%' THEN
            TRY_CAST(REGEXP_REPLACE(REGEXP_SUBSTR(data:"Mức giá"::VARCHAR, '[0-9.,]+'), ',', '.') AS FLOAT) * 1000000000
        WHEN data:"Mức giá"::VARCHAR LIKE '%triệu%' THEN
            TRY_CAST(REGEXP_REPLACE(REGEXP_SUBSTR(data:"Mức giá"::VARCHAR, '[0-9.,]+'), ',', '.') AS FLOAT) * 1000000
        ELSE NULL
    END AS price,
    -- Parse area (remove "m²" and convert)
    TRY_CAST(REGEXP_REPLACE(REGEXP_SUBSTR(data:"Diện tích"::VARCHAR, '[0-9.,]+'), ',', '.') AS FLOAT) AS area,
    -- Parse bedrooms
    TRY_CAST(REGEXP_SUBSTR(data:"Số phòng ngủ"::VARCHAR, '[0-9]+') AS INT) AS bedrooms,
    -- Parse bathrooms/toilets
    TRY_CAST(REGEXP_SUBSTR(data:"Số toilet"::VARCHAR, '[0-9]+') AS INT) AS bathrooms,
    -- Location/address
    TRIM(data:"Địa chỉ"::VARCHAR) AS location,
    -- City (extract from address - last part after comma)
    TRIM(REGEXP_SUBSTR(data:"Địa chỉ"::VARCHAR, '[^,]+$')) AS city,
    -- District
    TRIM(data:"Huyện"::VARCHAR) AS district,
    -- Coordinates
    TRY_CAST(data:"Latitude"::VARCHAR AS FLOAT) AS latitude,
    TRY_CAST(data:"Longitude"::VARCHAR AS FLOAT) AS longitude,
    -- Description/title
    TRIM(data:"Tiêu đề"::VARCHAR) AS description,
    -- Posted date (not available in data, use NULL)
    NULL AS posted_date,
    -- Calculate data quality score (0.0 - 1.0)
    (
        CASE WHEN data:"Mức giá" IS NOT NULL AND data:"Mức giá"::VARCHAR NOT LIKE '%Thỏa thuận%' THEN 0.2 ELSE 0 END +
        CASE WHEN data:"Diện tích" IS NOT NULL THEN 0.2 ELSE 0 END +
        CASE WHEN data:"Địa chỉ" IS NOT NULL THEN 0.2 ELSE 0 END +
        CASE WHEN data:"Số phòng ngủ" IS NOT NULL THEN 0.2 ELSE 0 END +
        CASE WHEN data:"Latitude" IS NOT NULL AND data:"Longitude" IS NOT NULL THEN 0.2 ELSE 0 END
    ) AS data_quality_score
FROM parsed
