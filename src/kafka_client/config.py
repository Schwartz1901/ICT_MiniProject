import os
from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer

load_dotenv()

# Get the directory where this config file is located
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SSL_CAFILE = os.path.join(BASE_DIR, "ca.pem")

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
SASL_PLAIN_USERNAME = os.getenv("KAFKA_USERNAME")
SASL_PLAIN_PASSWORD = os.getenv("KAFKA_PASSWORD")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "data_ingestion")

# Determine if SSL authentication is needed (cloud vs local)
USE_SSL = bool(SASL_PLAIN_USERNAME and SASL_PLAIN_PASSWORD)

# Lazy-loaded instances
_consumer = None
_producer = None


def _get_kafka_config():
    """Get Kafka configuration based on environment."""
    config = {
        "bootstrap_servers": BOOTSTRAP_SERVERS,
    }

    if USE_SSL:
        # Cloud Kafka (Aiven) with SASL_SSL
        config.update({
            "security_protocol": "SASL_SSL",
            "sasl_mechanism": "SCRAM-SHA-256",
            "sasl_plain_username": SASL_PLAIN_USERNAME,
            "sasl_plain_password": SASL_PLAIN_PASSWORD,
            "ssl_cafile": SSL_CAFILE,
        })
    else:
        # Local Kafka (Docker) without authentication
        config.update({
            "security_protocol": "PLAINTEXT",
        })

    return config


def get_consumer():
    """Get or create the Kafka consumer instance."""
    global _consumer
    if _consumer is None:
        config = _get_kafka_config()
        _consumer = KafkaConsumer(
            TOPIC_NAME,
            auto_offset_reset="earliest",
            client_id="CONSUMER_CLIENT_ID",
            group_id="CONSUMER_GROUP_ID",
            **config
        )
    return _consumer


def get_producer():
    """Get or create the Kafka producer instance."""
    global _producer
    if _producer is None:
        config = _get_kafka_config()
        _producer = KafkaProducer(**config)
    return _producer
