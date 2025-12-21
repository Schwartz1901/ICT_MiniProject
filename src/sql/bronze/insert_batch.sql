INSERT INTO RAW_DB.LANDING.BRONZE_REAL_ESTATE (id, raw_data, kafka_partition, kafka_offset)
VALUES (%s, PARSE_JSON(%s), %s, %s)
