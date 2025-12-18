import json
import logging
from kafka_client.config import get_producer, TOPIC_NAME
from mongo.config import get_collection, close_connection

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
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


if __name__ == "__main__":
    producer = None
    try:
        producer = get_producer()
        produce_to_kafka()
    except Exception as e:
        logger.error(f"Error producing to Kafka: {e}", exc_info=True)
    finally:
        close_connection()
        if producer:
            producer.close()
        logger.info("Connections closed")
