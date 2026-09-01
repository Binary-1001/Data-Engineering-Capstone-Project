# --- Kafka ---
from kafka import KafkaProducer    # the producer client that connects to Redpanda
from kafka.errors import KafkaError # catches failures when sending messages

# --- Serialization ---
import json                         # converts your dict payload to JSON string

# --- Time ---
from datetime import datetime       # generates the timestamp in your test payload

# --- Environment ---
import os                           # reads USER env variable to detect dev vs prod

# --- Logging ---
import logging                      # same logging pattern as forex and crm/main.

import time


logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logging.getLogger('kafka').setLevel(level=logging.ERROR)

# Environment detection
environment = 'dev' if os.getenv('USER', '') != '' else 'prod'
logger.info(f"Environment: {environment}")

# Broker config
if environment == 'dev':
    KAFKA_SERVERS = ['localhost:19092', 'localhost:29092', 'localhost:39092']
else:
    KAFKA_SERVERS = ['redpanda-0:9092', 'redpanda-1:9092', 'redpanda-2:9092']

logger.info(f"Kafka servers: {KAFKA_SERVERS}")

# Topic config
TOPIC_ACCOUNTS  = 'crm.accounts'
TOPIC_ADDRESSES = 'crm.addresses'
TOPIC_DEVICES   = 'crm.devices'

logger.info(f"Target topic: {TOPIC_ACCOUNTS}")


def connect_to_redpanda():
    """
    THis function connects to the redpanda using KafkaProducer() and returns the producer
    """
    producer = KafkaProducer(
        bootstrap_servers = KAFKA_SERVERS,
        key_serializer = lambda v : json.dumps(v).encode("utf-8"),
        value_serializer = lambda v : json.dumps(v).encode("utf-8")
    )
    logger.info("Connected to Redpanda successfully...")

    return producer

def produce_to_redpanda(producer,topic,key,record):
    """
    This function reads the internal database log and sends them to redpanda as topic[events]
    """
    for retry in range(100):
        future = producer.send(topic , key=key , value=record)
        try:
            rm = future.get(timeout=10)
            logger.info(f"Message sent to Kafka topic successfully - "
                       f"topic={rm.topic} "
                       f"partition={rm.partition} "
                       f"offset={rm.offset}")
            
            return

        except KafkaError as ke:
            logger.error(f"Attempt [{retry+1}] failed to produce message: {ke}")
            time.sleep(0.5)

    raise Exception(f"Failed to produce message after 100 attempts")


def build_test_payload():
    return {
        "account_id": 12345,
        "owner_name": "Test User",
        "email": "testuser@wtc.com",
        "phone_number": "27821234567",
        "modified_ts": datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'),
        "event_type": "INSERT"
    }

if __name__ == "__main__":
    logger.info("Starting CRM KAFKA producer test....")

    producer = None

    try:
        #1.Connect
        producer = connect_to_redpanda()

        #2.Build payload
        payload = build_test_payload()
        logger.info(f"TEST payload built: {payload}")

        #3.Build message key
        key=f"account-{payload['account_id']}"
        logger.info(f"Message key: {key}")

        #4.Producer to redpanda
        produce_to_redpanda(
            producer=producer,
            topic=TOPIC_ACCOUNTS,
            key=key,
            record=payload
        )

    except Exception as e:
        logger.error(f"Test failed: {e}")
        raise

    finally:
        if producer:
            producer.flush(timeout=10)
            producer.close()
            logger.info("Producer closed")
            logger.info(r'http://localhost:18084')

    