import json
import logging
from kafka_client.config import get_consumer, TOPIC_NAME

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def consume_from_kafka():
    """Consume messages from Kafka topic and log them."""
    consumer = get_consumer()

    logger.info(f"Starting consumer for topic '{TOPIC_NAME}'")
    logger.info("Waiting for messages... (Press Ctrl+C to stop)")

    message_count = 0
    for message in consumer:
        message_count += 1
        try:
            value = json.loads(message.value.decode("utf-8"))
            logger.info(f"Message {message_count} | Partition: {message.partition} | Offset: {message.offset}")
            logger.info(f"Content: {value}")
        except json.JSONDecodeError:
            logger.warning(f"Message {message_count}: Unable to decode JSON - {message.value}")


if __name__ == "__main__":
    consumer = None
    try:
        consume_from_kafka()
    except KeyboardInterrupt:
        logger.info("Consumer stopped by user")
    except Exception as e:
        logger.error(f"Error consuming from Kafka: {e}", exc_info=True)
    finally:
        consumer = get_consumer()
        if consumer:
            consumer.close()
        logger.info("Consumer closed")
