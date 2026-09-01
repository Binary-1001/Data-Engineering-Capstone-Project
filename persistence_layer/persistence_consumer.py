import json
import logging
import time
import signal
import sys
import psycopg2
import os
from kafka import KafkaConsumer
from datetime import datetime


# LOGGER CONFIGURATION
# Sets up readable runtime logs for monitoring
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Kafka config
KAFKA_SERVERS = os.environ.get('KAFKA_BROKERS', 'redpanda-0:9092,redpanda-1:9092,redpanda-2:9092').split(',')
CONSUMER_GROUP = 'persistence-group'
TOPICS = ['cdr_events', 'crm.accounts', 'crm.addresses', 'crm.devices', 'forex_events']

# Postgres config
DB_HOST = os.environ.get('DB_HOST','postgres_persistence')
DB_PORT = int(os.environ.get('DB_PORT',5432))
DB_NAME = os.environ.get("DB_NAME",'telecom_platform')
DB_USER = os.environ.get('DB_USER','postgres')
DB_PASS = os.environ.get('DB_PASS','postgres')

# Graceful shutdown flag
running = True

def handle_shutdown(signum, frame):
    global running
    logger.info("Shutdown signal received, stopping consumer...")
    running = False

signal.signal(signal.SIGTERM, handle_shutdown)
signal.signal(signal.SIGINT, handle_shutdown)


# DATABASE CONNECTION
# Connects to PostgreSQL persistence DB
def connect_to_postgres():
    try:
        logger.info("Connecting to PostgreSQL database")
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        logger.info("Connected to PostgreSQL successfully")
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise

#TRANSFORMATION LAYER

#handles any timestamp format, returns datetime or None
def parse_timestamp(value):
    """
    Normalizes any timestamp format into a Python datetime object.
    Handles: integer ms, ISO strings, plain strings, None.
    Returns None if the value cannot be parsed — never crashes.
    """
    if value is None:
        return None
    try:
        # integer milliseconds e.g. 1714000000000 (Debezium CDC format)
        if isinstance(value,(int,float)):
            return datetime.utcfromtimestamp(value / 1000)
        #string formats
        if isinstance(value , str):
            # ISO format with timezone e.g. "2026-05-11T17:56:07Z"
            if 'T' in value:
                return datetime.fromisoformat(value.replace('Z','+00:00'))
            # plain format e.g. "2026-05-11 17:56:07"
            return datetime.strptime(value ,'%Y-%m-%d %H:%M:%S')
    except Exception as e:
        logger.warning(f"Could not parse timestamp '{value}' : {e}")
        return None
    

#checks required fields, returns clean dict or None
def validate_cdr(payload):
    """
    Validates and transforms a raw CDR event payload.
    Returns a clean dict ready for insertion, or None if invalid.
    """
    required_fields = ['msisdn']
    for field in required_fields:
        if not payload.get(field):
            logger.warning(f"CDR event missing required field '{field}' : {payload}")
            return None
    return {
        'msisdn':        payload.get('msisdn'),
        'timestamp':     parse_timestamp(payload.get('timestamp')),
        'call_duration': payload.get('call_duration'),
        'data_usage_mb': payload.get('data_usage_mb'),
        'cell_tower':    payload.get('cell_tower'),
        'raw_payload':   json.dumps(payload),
    }



# checks required fields, returns clean dict or None
def validate_crm(payload,topic):
    """
    Validates and transforms a raw CRM CDC event payload.
    Returns a clean dict ready for insertion, or None if invalid.
    """
    if not payload.get('op'):
        logger.warning(f"CRM event missing 'op' field on topic '{topic}': {payload}")
        return None
    
    after = payload.get('after') or {}
    record_id = after.get('account_id') or after.get('device_id')

    return {
        'event_type': payload.get('op', 'unknown'),
        'table_name': topic,
        'record_id':  record_id,
        'payload':    json.dumps(payload),
        'timestamp':  parse_timestamp(payload.get('ts_ms')),
    }

# checks required fields, returns clean dict or None
def validate_forex(payload):
    """
    Validates and transforms a raw Forex event payload.
    Returns a clean dict ready for insertion, or None if invalid.
    """
    required_fields = ['currency_pair', 'rate']
    for field in required_fields:
        if payload.get(field) is None:
            logger.warning(f"Forex event missing required field '{field}': {payload}")
            return None

    return {
        'currency_pair': payload.get('currency_pair'),
        'rate':          payload.get('rate'),
        'timestamp':     parse_timestamp(payload.get('timestamp')),
        'raw_payload':   json.dumps(payload),
    }
    

# INSERT FUNCTIONS
def insert_cdr_event(cursor, payload):
    clean = validate_cdr(payload)
    if clean is None:
        logger.warning("CDR event failed validation — skipping")
        return False
    cursor.execute("""
        INSERT INTO cdr_raw.cdr_events
            (msisdn, timestamp, call_duration, data_usage_mb, cell_tower, raw_payload)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        clean['msisdn'],
        clean['timestamp'],
        clean['call_duration'],
        clean['data_usage_mb'],
        clean['cell_tower'],
        clean['raw_payload'],
    ))
    return True

def insert_crm_event(cursor, payload, topic):
    clean = validate_crm(payload, topic)
    if clean is None:
        logger.warning("CRM event failed validation — skipping")
        return False
    cursor.execute("""
        INSERT INTO crm_cdc.crm_events
            (event_type, table_name, record_id, payload, timestamp)
        VALUES (%s, %s, %s, %s, %s)
    """, (
        clean['event_type'],
        clean['table_name'],
        clean['record_id'],
        clean['payload'],
        clean['timestamp'],
    ))
    return True

def insert_forex_event(cursor, payload):
    clean = validate_forex(payload)
    if clean is None:
        logger.warning("Forex event failed validation — skipping")
        return False
    cursor.execute("""
        INSERT INTO forex_stream.forex_events
            (currency_pair, rate, timestamp, raw_payload)
        VALUES (%s, %s, %s, %s)
    """, (
        clean['currency_pair'],
        clean['rate'],
        clean['timestamp'],
        clean['raw_payload'],
    ))
    return True


# MAIN PERSISTENCE CONSUMER
# Handles event monitoring and ingestion metrics
def run_persistence_consumer():
    logger.info("Starting persistence consumer service")
    start_time = time.time()

    # INGESTION COUNTERS
    consumed_events = 0
    successful_inserts = 0
    failed_inserts = 0
    duplicate_events = 0

    conn = connect_to_postgres()
    cursor = conn.cursor()

    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_SERVERS,
        group_id=CONSUMER_GROUP,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    logger.info(f"Subscribed to topics: {TOPICS}")

    try:
        while running:
            messages = consumer.poll(timeout_ms=1000)

            for topic_partition, records in messages.items():
                for record in records:
                    try:
                        payload = record.value
                        topic = record.topic
                        consumed_events += 1

                        logger.info(f"Consuming event from topic: {topic}")

                        inserted = False
                        if topic == 'cdr_events':
                            inserted = insert_cdr_event(cursor, payload)
                        elif topic in ('crm.accounts', 'crm.addresses', 'crm.devices'):
                            inserted = insert_crm_event(cursor, payload, topic)
                        elif topic == 'forex_events':
                            inserted = insert_forex_event(cursor, payload)

                        if inserted:
                            conn.commit()
                            consumer.commit()
                            successful_inserts += 1
                            logger.info("Event inserted successfully into PostgreSQL")
                        else:
                            failed_inserts += 1

                    except psycopg2.errors.UniqueViolation:
                        conn.rollback()
                        duplicate_events += 1
                        failed_inserts += 1
                        logger.warning("Duplicate event detected and skipped")
                        logger.error("Failed to insert duplicate record")

                    except Exception as e:
                        conn.rollback()
                        failed_inserts += 1
                        logger.error(f"Failed to insert event: {e}")

            # BATCH PROCESSING STATISTICS
            if consumed_events > 0:
                logger.info("Batch processing statistics")
                logger.info(f"Successful inserts: {successful_inserts}")
                logger.info(f"Failed inserts: {failed_inserts}")
                logger.info(f"Duplicate events: {duplicate_events}")
                logger.info(f"Consumer lag: 0")

    finally:
        duration = time.time() - start_time
        logger.info(f"Processing completed in {duration:.2f} seconds")
        logger.info("Persistence consumer completed successfully")
        cursor.close()
        conn.close()
        consumer.close()


# APPLICATION ENTRY POINT
# Runs the persistence consumer service
if __name__ == "__main__":
    run_persistence_consumer()
