import os
from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer

load_dotenv()

# Get the directory where this config file is located
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SSL_CAFILE = os.path.join(BASE_DIR, "ca.pem")

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
SASL_SSL = "SASL_SSL"
SASL_PLAIN_USERNAME = os.getenv("KAFKA_USERNAME")
SASL_PLAIN_PASSWORD = os.getenv("KAFKA_PASSWORD")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "data_ingestion")
SASL_MECHANISM = "SCRAM-SHA-256"

# Lazy-loaded instances
_consumer = None
_producer = None


def get_consumer():
    """Get or create the Kafka consumer instance."""
    global _consumer
    if _consumer is None:
        _consumer = KafkaConsumer(
            TOPIC_NAME,
            auto_offset_reset="earliest",
            bootstrap_servers=BOOTSTRAP_SERVERS,
            client_id="CONSUMER_CLIENT_ID",
            group_id="CONSUMER_GROUP_ID",
            sasl_mechanism=SASL_MECHANISM,
            sasl_plain_username=SASL_PLAIN_USERNAME,
            sasl_plain_password=SASL_PLAIN_PASSWORD,
            security_protocol=SASL_SSL,
            ssl_cafile=SSL_CAFILE,
        )
    return _consumer


def get_producer():
    """Get or create the Kafka producer instance."""
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            sasl_mechanism=SASL_MECHANISM,
            sasl_plain_username=SASL_PLAIN_USERNAME,
            sasl_plain_password=SASL_PLAIN_PASSWORD,
            security_protocol=SASL_SSL,
            ssl_cafile=SSL_CAFILE,
        )
    return _producer
