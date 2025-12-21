INSERT INTO RAW_DB.LANDING.BRONZE_REAL_ESTATE (id, raw_data, kafka_partition, kafka_offset)
SELECT %s, PARSE_JSON(%s), %s, %s
