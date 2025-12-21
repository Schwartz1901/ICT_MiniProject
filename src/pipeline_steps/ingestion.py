"""
Ingestion Layer - MongoDB to Kafka Producer

Reads documents from MongoDB in batches and produces them to Kafka
for downstream processing in the Medallion architecture.
"""

import json
import logging
from kafka_client.config import get_producer, TOPIC_NAME
from mongo.config import get_collection, close_connection as close_mongo

logger = logging.getLogger(__name__)

BATCH_SIZE = 100


def produce_to_kafka():
    """Read documents from MongoDB in batches and produce them to Kafka."""
    collection = get_collection("details", "Bigdata")
    producer = get_producer()

    logger.info("Connecting to MongoDB database 'Bigdata', collection 'details'")
    logger.info(f"Producing messages to Kafka topic '{TOPIC_NAME}' (batch size: {BATCH_SIZE})")

    total_count = 0
    batch_num = 0

    while True:
        skip = batch_num * BATCH_SIZE
        batch = list(collection.find().skip(skip).limit(BATCH_SIZE))

        if not batch:
            break

        batch_num += 1
        batch_count = 0

        for document in batch:
            # Convert ObjectId to string for JSON serialization
            document["_id"] = str(document["_id"])

            # Serialize document to JSON bytes
            message = json.dumps(document).encode("utf-8")

            # Send to Kafka
            producer.send(TOPIC_NAME, value=message)
            batch_count += 1

        # Flush after each batch
        producer.flush()
        total_count += batch_count
        logger.info(f"Batch {batch_num}: produced {batch_count} documents (total: {total_count})")

    logger.info(f"Successfully produced {total_count} documents to Kafka in {batch_num} batches")
    return total_count


def run():
    """Run ingestion pipeline step."""
    producer = None
    try:
        logger.info("Starting MongoDB to Kafka ingestion...")
        producer = get_producer()
        count = produce_to_kafka()
        logger.info("Ingestion completed successfully")
        return count
    except Exception as e:
        logger.error(f"Error in ingestion: {e}", exc_info=True)
        raise
    finally:
        close_mongo()
        if producer:
            producer.close()
