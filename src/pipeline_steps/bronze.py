"""
Bronze Layer - Raw Data Ingestion

Consumes data from Kafka and loads it into Snowflake Bronze layer
with minimal transformation (raw JSON storage).
Uses thread pool for parallel Snowflake inserts.
"""

import os
import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from kafka_client.config import get_consumer, TOPIC_NAME
from snowflake_client.config import get_connection, close_connection
from sql import load_query

load_dotenv()

logger = logging.getLogger(__name__)

BATCH_SIZE = int(os.getenv("BRONZE_BATCH_SIZE", "100"))
NUM_WORKERS = int(os.getenv("BRONZE_WORKERS", "16"))


def create_bronze_table():
    """Create Bronze layer table in Snowflake if not exists."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(load_query("bronze", "create_table"))
    cursor.close()
    logger.info("Bronze table created/verified: RAW_DB.LANDING.BRONZE_REAL_ESTATE")


class ThreadSafeCounter:
    """Thread-safe counter for tracking Bronze layer progress."""

    def __init__(self):
        self._value = 0
        self._lock = threading.Lock()

    def increment(self, amount=1):
        """Increment counter by amount."""
        with self._lock:
            self._value += amount
            return self._value

    @property
    def value(self):
        with self._lock:
            return self._value


def insert_batch_worker(batch_data, batch_num, counter):
    """
    Worker function to insert a batch of records to Snowflake.

    Args:
        batch_data: List of tuples (doc_id, raw_json, partition, offset)
        batch_num: Batch number for logging
        counter: Thread-safe counter

    Returns:
        Tuple of (batch_num, inserted_count)
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.executemany(load_query("bronze", "insert_batch"), batch_data)
        cursor.close()
        inserted = len(batch_data)
        total = counter.increment(inserted)
        return batch_num, inserted, total
    except Exception as e:
        logger.error(f"Batch {batch_num} insert failed: {e}")
        return batch_num, 0, counter.value


def insert_batch(batch):
    """Insert batch of records to Snowflake Bronze layer (legacy single-threaded)."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.executemany(load_query("bronze", "insert_batch"), batch)
    cursor.close()
    return len(batch)


def load_to_bronze(timeout_seconds=60, reset_offset=True):
    """
    Consume messages from Kafka and load to Snowflake Bronze layer using thread pool.

    Args:
        timeout_seconds: Maximum time to wait for messages
        reset_offset: If True, seek to beginning of topic to reprocess all messages

    Returns:
        Total number of records loaded
    """
    consumer = get_consumer()
    create_bronze_table()

    logger.info(f"Starting Bronze loader - consuming from topic '{TOPIC_NAME}'")
    logger.info(f"Sinking to Snowflake BRONZE_REAL_ESTATE table (batch size: {BATCH_SIZE})")
    logger.info(f"Using {NUM_WORKERS} worker threads for parallel inserts")

    # Wait for partition assignment (consumer group join can take a few seconds)
    partitions = set()
    max_wait = 30  # seconds
    wait_start = time.time()
    while not partitions and (time.time() - wait_start) < max_wait:
        consumer.poll(timeout_ms=1000)
        partitions = consumer.assignment()
        if not partitions:
            logger.info("Waiting for partition assignment...")

    if not partitions:
        logger.error("Failed to get partition assignment")
        consumer.close()
        return 0

    logger.info(f"Assigned to {len(partitions)} partition(s)")

    # Seek to beginning to reprocess all messages
    if reset_offset:
        consumer.seek_to_beginning(*partitions)
        logger.info(f"Reset consumer offset to beginning for {len(partitions)} partition(s)")

    # Collect all batches first (Kafka consumer is not thread-safe)
    all_batches = []
    batch = []
    batch_num = 0
    start_time = time.time()

    logger.info("Consuming messages from Kafka...")
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

        # Create batch when full
        if len(batch) >= BATCH_SIZE:
            batch_num += 1
            all_batches.append((batch_num, batch))
            batch = []

    # Add remaining records as final batch
    if batch:
        batch_num += 1
        all_batches.append((batch_num, batch))

    consumer.close()

    if not all_batches:
        logger.info("No batches to process")
        return 0

    total_records = sum(len(b[1]) for b in all_batches)
    logger.info(f"Consumed {len(all_batches)} batches ({total_records} records) from Kafka")

    # Process batches in parallel using thread pool
    counter = ThreadSafeCounter()

    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        # Submit all batches for processing
        futures = {
            executor.submit(insert_batch_worker, batch_data, batch_num, counter): batch_num
            for batch_num, batch_data in all_batches
        }

        # Collect results as they complete
        for future in as_completed(futures):
            batch_num = futures[future]
            try:
                result_batch_num, inserted, total = future.result()
                if inserted > 0:
                    logger.info(f"Batch {result_batch_num}: loaded {inserted} records to Bronze (total: {total})")
            except Exception as e:
                logger.error(f"Batch {batch_num} failed: {e}")

    total_count = counter.value
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
