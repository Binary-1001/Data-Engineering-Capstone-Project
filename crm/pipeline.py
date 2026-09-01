import json
import logging
import time

from sqlalchemy import create_engine, text
from kafka import KafkaProducer


#Config-setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#servers
KAFKA_SERVERS = ['localhost:19092', 'localhost:29092', 'localhost:39092'] 

#topics
TOPIC_ACCOUNTS  = 'crm.accounts'
TOPIC_ADDRESSES = 'crm.addresses'
TOPIC_DEVICES   = 'crm.devices'

#DB config
DB_URL = 'postgresql+psycopg2://postgres:postgres@localhost:15432/wtc_prod'

def connect_to_postgres():
    """This function connects to the postgres database & returns the SQLAlchemy connection"""
    engine = create_engine(DB_URL)
    connection = engine.connect()
    logger.info("Connected to PostgreSQL successfully")
    return connection


def extract_accounts(connection):

    """This function extract raw data from the crm.accounts table in the database
    given the schema -> crm_system.accounts columns:
        account_id, owner_name, email, phone_number, modified_ts"""
    
    result = connection.execute(text("SELECT * FROM crm_system.accounts"))
    rows = result.fetchall()
    logger.info(f"Extracted {len(rows)} account rows")
    return rows


def extract_addresses(connection):
    """This function extract raw data from the crm.address table in the database
    given the schema -> crm_system.addresses columns:
        account_id, street_address, city, state, postal_code, country, modified_ts"""
    result = connection.execute(text("SELECT * FROM crm_system.addresses"))
    rows = result.fetchall()
    logger.info(f"Extracted {len(rows)} address rows")
    return rows


def extract_devices(connection):
    """This function extract raw data from the crm.devices table in the database
    given the schema -> crm_system.devices columns:
        device_id, account_id, device_name, device_type, device_os, modified_ts"""
    result = connection.execute(text("SELECT * FROM crm_system.devices"))
    rows = result.fetchall()
    logger.info(f"Extracted {len(rows)} device rows")
    return rows


def transform_account(row):
    # row = (account_id, owner_name, email, phone_number, modified_ts)
    """This function transforms the extracted data row into a clean dict form"""
    return {
        "account_id":   row[0],
        "owner_name":   row[1],
        "email":        row[2],
        "phone_number": row[3],
        "modified_ts":  str(row[4]),
        "source":       "postgresql",
    }


def transform_address(row):
    # row = (account_id, street_address, city, state, postal_code, country, modified_ts)
    """This function transforms the extracted data row into a clean dict form"""
    return {
        "account_id":     row[0],
        "street_address": row[1],
        "city":           row[2],
        "state":          row[3],
        "postal_code":    row[4],
        "country":        row[5],
        "modified_ts":    str(row[6]),
        "source":         "postgresql",
    }


def transform_device(row):
    # row = (device_id, account_id, device_name, device_type, device_os, modified_ts)
    """This function transforms the extracted data row into a clean dict form"""
    return {
        "device_id":   row[0],
        "account_id":  row[1],
        "device_name": row[2],
        "device_type": row[3],
        "device_os":   row[4],
        "modified_ts": str(row[5]),
        "source":      "postgresql",
    }

def build_producer():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )
    logger.info("Connected to Redpanda successfully")
    return producer


def run_pipeline():
    """This function runs the whole pipeline for each table and pushes each change to redpanda"""
    connection = connect_to_postgres()
    producer = build_producer()

    start_time = time.time()

    processed_accounts = 0
    processed_addresses = 0
    processed_devices = 0
    
    try:
        #accounts
        account_rows = extract_accounts(connection)
        for row in account_rows:
            payload = transform_account(row)
            key = f"account-{row[0]}"
            producer.send(TOPIC_ACCOUNTS , key=key , value=payload)
        logger.info(f"Published {len(account_rows)} accounts to {TOPIC_ACCOUNTS}")

        #addresses
        address_rows = extract_addresses(connection)
        for row in address_rows:
            payload = transform_address(row)
            key = f"address-{row[0]}"
            producer.send(TOPIC_ADDRESSES, key=key,value=payload)
        logger.info(f"Published {len(address_rows)} addresses to {TOPIC_ADDRESSES}")

        #devices
        device_rows = extract_devices(connection)
        for row in device_rows:
            payload = transform_device(row)
            key = f"device-{row[0]}"
            producer.send(TOPIC_DEVICES, key=key , value=payload)
        logger.info(f"Published {len(device_rows)} devices to {TOPIC_DEVICES}")


        producer.flush()

        duration = time.time() - start_time
        logger.info(f"Pipeline processing completed in {duration:.2f} seconds")


        logger.info("Pipeline completed successfully")



    finally:
        connection.close()
        producer.close()

if __name__ == "__main__":
    run_pipeline()