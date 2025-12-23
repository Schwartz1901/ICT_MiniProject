"""
Ingestion Layer - MongoDB to Kafka Producer

Reads documents from MongoDB in batches and produces them to Kafka
for downstream processing in the Medallion architecture.
Skips documents that have already been ingested to avoid duplicates.
Uses thread pool for parallel ingestion.
"""

import json
import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from dotenv import load_dotenv
from kafka_client.config import get_producer, TOPIC_NAME
from mongo.config import get_collection, close_connection as close_mongo
from snowflake_client.config import get_connection, close_connection as close_snowflake
from sql import load_query

load_dotenv()

logger = logging.getLogger(__name__)

BATCH_SIZE = 100
MAX_RECORDS = int(os.getenv("MAX_INGESTION_RECORDS", "10000"))
NUM_WORKERS = int(os.getenv("INGESTION_WORKERS", "16"))


def get_existing_ids():
    """Fetch IDs that have already been ingested to Bronze layer."""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(load_query("bronze", "get_existing_ids"))
        results = cursor.fetchall()
        cursor.close()
        existing_ids = {row[0] for row in results}
        logger.info(f"Found {len(existing_ids)} existing records in Bronze layer")
        return existing_ids
    except Exception as e:
        logger.warning(f"Could not fetch existing IDs (table may not exist yet): {e}")
        return set()


class ThreadSafeCounter:
    """Thread-safe counter for tracking ingestion progress."""

    def __init__(self, max_value=None):
        self._value = 0
        self._lock = threading.Lock()
        self._max_value = max_value

    def increment(self, amount=1):
        """Increment counter, returns True if successful, False if max reached."""
        with self._lock:
            if self._max_value and self._value >= self._max_value:
                return False
            self._value += amount
            return True

    def try_increment(self):
        """Try to increment by 1, returns True if under max, False otherwise."""
        with self._lock:
            if self._max_value and self._value >= self._max_value:
                return False
            self._value += 1
            return True

    @property
    def value(self):
        with self._lock:
            return self._value

    def is_max_reached(self):
        with self._lock:
            return self._max_value and self._value >= self._max_value


def process_batch(batch_data, producer, existing_ids, skip_duplicates, counter, skipped_counter):
    """
    Process a batch of documents and send to Kafka.

    Args:
        batch_data: Tuple of (batch_num, documents)
        producer: Kafka producer instance
        existing_ids: Set of existing document IDs to skip
        skip_duplicates: Whether to skip duplicates
        counter: Thread-safe counter for produced records
        skipped_counter: Thread-safe counter for skipped records

    Returns:
        Tuple of (batch_num, produced_count, skipped_count)
    """
    batch_num, documents = batch_data
    produced = 0
    skipped = 0

    for document in documents:
        # Check if max records reached
        if counter.is_max_reached():
            break

        # Convert ObjectId to string for JSON serialization
        doc_id = str(document["_id"])
        document["_id"] = doc_id

        # Skip if already exists in Bronze layer
        if skip_duplicates and doc_id in existing_ids:
            skipped += 1
            skipped_counter.increment()
            continue

        # Try to increment counter (respects max limit)
        if not counter.try_increment():
            break

        # Serialize document to JSON bytes
        message = json.dumps(document).encode("utf-8")

        # Send to Kafka (thread-safe in kafka-python)
        producer.send(TOPIC_NAME, value=message)
        produced += 1

    return batch_num, produced, skipped


def produce_to_kafka(skip_duplicates=True):
    """
    Read documents from MongoDB in batches and produce them to Kafka using thread pool.

    Args:
        skip_duplicates: If True, skip documents already in Bronze layer

    Returns:
        Total number of new documents produced
    """
    collection = get_collection("details", "Bigdata")
    producer = get_producer()

    # Get existing IDs to skip duplicates
    existing_ids = set()
    if skip_duplicates:
        existing_ids = get_existing_ids()

    logger.info("Connecting to MongoDB database 'Bigdata', collection 'details'")
    logger.info(f"Producing messages to Kafka topic '{TOPIC_NAME}' (batch size: {BATCH_SIZE}, max: {MAX_RECORDS})")
    logger.info(f"Using {NUM_WORKERS} worker threads for parallel ingestion")
    if skip_duplicates:
        logger.info(f"Duplicate detection enabled - will skip {len(existing_ids)} existing records")

    # Thread-safe counters
    counter = ThreadSafeCounter(max_value=MAX_RECORDS)
    skipped_counter = ThreadSafeCounter()

    # Prefetch batches from MongoDB (MongoDB cursor is not thread-safe)
    batches = []
    batch_num = 0

    logger.info("Fetching documents from MongoDB...")
    while True:
        skip = batch_num * BATCH_SIZE
        batch = list(collection.find().skip(skip).limit(BATCH_SIZE))

        if not batch:
            break

        batch_num += 1
        batches.append((batch_num, batch))

        # Stop fetching if we have enough documents
        total_docs = sum(len(b[1]) for b in batches)
        if total_docs >= MAX_RECORDS + len(existing_ids):
            break

    logger.info(f"Fetched {len(batches)} batches ({sum(len(b[1]) for b in batches)} documents)")

    # Process batches in parallel using thread pool
    results = []
    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        # Submit all batches for processing
        futures = {
            executor.submit(
                process_batch,
                batch_data,
                producer,
                existing_ids,
                skip_duplicates,
                counter,
                skipped_counter
            ): batch_data[0]
            for batch_data in batches
        }

        # Collect results as they complete
        for future in as_completed(futures):
            batch_num = futures[future]
            try:
                result = future.result()
                results.append(result)
                batch_num, produced, skipped = result
                if produced > 0 or skipped > 0:
                    logger.info(f"Batch {batch_num}: produced {produced}, skipped {skipped} duplicates (total: {counter.value})")
            except Exception as e:
                logger.error(f"Batch {batch_num} failed: {e}")

            # Check if max reached
            if counter.is_max_reached():
                logger.info(f"Reached maximum ingestion limit: {MAX_RECORDS}")
                break

    # Flush all pending messages
    producer.flush()

    total_produced = counter.value
    total_skipped = skipped_counter.value

    logger.info(f"Ingestion complete: {total_produced} new documents, {total_skipped} duplicates skipped")
    return total_produced


def run():
    """Run ingestion pipeline step."""
    producer = None
    try:
        logger.info("Starting MongoDB to Kafka ingestion...")
        producer = get_producer()
        count = produce_to_kafka(skip_duplicates=True)
        logger.info("Ingestion completed successfully")
        return count
    except Exception as e:
        logger.error(f"Error in ingestion: {e}", exc_info=True)
        raise
    finally:
        close_mongo()
        close_snowflake()
        if producer:
            producer.close()
