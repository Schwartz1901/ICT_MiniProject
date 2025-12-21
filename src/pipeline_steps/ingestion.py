"""
Ingestion Layer - MongoDB to Kafka Producer

Reads documents from MongoDB in batches and produces them to Kafka
for downstream processing in the Medallion architecture.
Skips documents that have already been ingested to avoid duplicates.
"""

import os
import json
import logging
from dotenv import load_dotenv
from kafka_client.config import get_producer, TOPIC_NAME
from mongo.config import get_collection, close_connection as close_mongo
from snowflake_client.config import get_connection, close_connection as close_snowflake
from sql import load_query

load_dotenv()

logger = logging.getLogger(__name__)

BATCH_SIZE = 100
MAX_RECORDS = int(os.getenv("MAX_INGESTION_RECORDS", "10000"))


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


def produce_to_kafka(skip_duplicates=True):
    """
    Read documents from MongoDB in batches and produce them to Kafka.

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
    if skip_duplicates:
        logger.info(f"Duplicate detection enabled - will skip {len(existing_ids)} existing records")

    total_count = 0
    skipped_count = 0
    batch_num = 0

    while True:
        # Check if max records reached
        if total_count >= MAX_RECORDS:
            logger.info(f"Reached maximum ingestion limit: {MAX_RECORDS}")
            break

        skip = batch_num * BATCH_SIZE
        batch = list(collection.find().skip(skip).limit(BATCH_SIZE))

        if not batch:
            break

        batch_num += 1
        batch_count = 0
        batch_skipped = 0

        for document in batch:
            # Convert ObjectId to string for JSON serialization
            doc_id = str(document["_id"])
            document["_id"] = doc_id

            # Skip if already exists in Bronze layer
            if skip_duplicates and doc_id in existing_ids:
                batch_skipped += 1
                continue

            # Check if we've reached max records
            if total_count >= MAX_RECORDS:
                break

            # Serialize document to JSON bytes
            message = json.dumps(document).encode("utf-8")

            # Send to Kafka
            producer.send(TOPIC_NAME, value=message)
            batch_count += 1
            total_count += 1

        # Flush after each batch
        producer.flush()
        skipped_count += batch_skipped

        if batch_count > 0 or batch_skipped > 0:
            logger.info(f"Batch {batch_num}: produced {batch_count}, skipped {batch_skipped} duplicates (total: {total_count})")

    logger.info(f"Ingestion complete: {total_count} new documents, {skipped_count} duplicates skipped")
    return total_count


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
