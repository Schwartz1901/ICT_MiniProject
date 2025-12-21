"""
Bronze Layer - Raw Data Ingestion

Consumes data from Kafka and loads it into Snowflake Bronze layer
with minimal transformation (raw JSON storage).
"""

import json
import logging
from kafka_client.config import get_consumer, TOPIC_NAME
from snowflake_client.config import get_connection, close_connection
from sql import load_query

logger = logging.getLogger(__name__)

BATCH_SIZE = 100


def create_bronze_table():
    """Create Bronze layer table in Snowflake if not exists."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(load_query("bronze", "create_table"))
    cursor.close()
    logger.info("Bronze table created/verified: RAW_DB.LANDING.BRONZE_REAL_ESTATE")


def insert_batch(batch):
    """Insert batch of records to Snowflake Bronze layer."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.executemany(load_query("bronze", "insert_batch"), batch)
    cursor.close()
    return len(batch)


def load_to_bronze(timeout_seconds=60):
    """
    Consume messages from Kafka and load to Snowflake Bronze layer.

    Args:
        timeout_seconds: Maximum time to wait for messages

    Returns:
        Total number of records loaded
    """
    import time

    consumer = get_consumer()
    create_bronze_table()

    logger.info(f"Starting Bronze loader - consuming from topic '{TOPIC_NAME}'")
    logger.info(f"Sinking to Snowflake BRONZE_REAL_ESTATE table (batch size: {BATCH_SIZE})")

    batch = []
    total_count = 0
    batch_num = 0
    start_time = time.time()

    while True:
        # Check timeout
        if time.time() - start_time > timeout_seconds:
            logger.info(f"Timeout reached after {timeout_seconds}s")
            break

        # Poll for messages
        msg_pack = consumer.poll(timeout_ms=5000, max_records=BATCH_SIZE)

        if not msg_pack:
            logger.info("No more messages available")
            break

        for tp, messages in msg_pack.items():
            for message in messages:
                try:
                    value = json.loads(message.value.decode("utf-8"))
                    doc_id = value.get("_id", "unknown")
                    raw_json = json.dumps(value)

                    batch.append((doc_id, raw_json, message.partition, message.offset))

                except json.JSONDecodeError as e:
                    logger.warning(f"Unable to decode JSON at offset {message.offset}: {e}")
                except Exception as e:
                    logger.error(f"Error processing message: {e}")

        # Insert batch
        if len(batch) >= BATCH_SIZE:
            batch_num += 1
            inserted = insert_batch(batch)
            total_count += inserted
            logger.info(f"Batch {batch_num}: loaded {inserted} records to Bronze (total: {total_count})")
            batch = []

    # Insert remaining records
    if batch:
        batch_num += 1
        inserted = insert_batch(batch)
        total_count += inserted
        logger.info(f"Batch {batch_num}: loaded {inserted} records to Bronze (total: {total_count})")

    consumer.close()
    logger.info(f"Bronze loader completed. Total records loaded: {total_count}")
    return total_count


def get_bronze_stats():
    """Get statistics about the Bronze layer data."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(load_query("bronze", "get_stats"))
    stats = cursor.fetchone()
    cursor.close()

    if stats[0] > 0:
        logger.info(f"Bronze Layer Stats:")
        logger.info(f"  - Total records: {stats[0]}")
        logger.info(f"  - Unique IDs: {stats[1]}")
        logger.info(f"  - First ingestion: {stats[2]}")
        logger.info(f"  - Last ingestion: {stats[3]}")

    return stats


def run(timeout_seconds=60):
    """Run Bronze layer pipeline step."""
    try:
        logger.info("Starting Bronze layer ingestion...")
        total = load_to_bronze(timeout_seconds=timeout_seconds)
        get_bronze_stats()
        logger.info("Bronze layer ingestion completed")
        return total
    except Exception as e:
        logger.error(f"Error in Bronze layer: {e}", exc_info=True)
        raise
    finally:
        close_connection()
