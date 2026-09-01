import json
import os
import time
import logging
from kafka import KafkaProducer
from kafka import KafkaConsumer
from kafka.errors import KafkaError


logging.basicConfig(level=logging.INFO, format='{"time": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "message": "%(message)s"}', datefmt='%Y-%m-%d %H:%M:%S ')
logger = logging.getLogger(__name__)

environment = 'dev' if os.getenv('USER', '') != '' else 'prod'

if environment == 'dev':
    KAFKA_SERVERS=['localhost:19092','localhost:29092','localhost:39092','localhost:49092','localhost:59092']
    
else:
    KAFKA_SERVERS=['redpanda-0:9092','redpanda-1:9092','redpanda-2:9092']

CDR_DATA_TOPIC='cdr-data'
CDR_VOICE_TOPIC='cdr-voice'
CDR_DATA_DLQ = 'cdr-data-dlq'
CDR_VOICE_DLQ = 'cdr-voice-dlq'
GROUP_ID = 'cdr-consumer-group'


def connect_to_kafka():
    for i in range(10):
        try:
            consumer = KafkaConsumer(
                CDR_DATA_TOPIC,
                CDR_VOICE_TOPIC,
                bootstrap_servers=KAFKA_SERVERS,
                group_id=GROUP_ID,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                value_deserializer=lambda x: x.decode('utf-8') if x is not None else None
            )
            logger.info("Consuming from Kafka")
            return consumer
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            time.sleep(1)

def deserialize_message(value,topic):
    try:
        fields = value.split(',')
        if topic == CDR_DATA_TOPIC:
            return {
                "msisdn": fields[0],
                "tower_id": fields[1],
                "up_bytes": fields[2],
                "down_bytes": fields[3],
                "data_type": fields[4],
                "ip_address": fields[5],
                "website_url": fields[6],
                "event_datetime": fields[7]}
        
        elif topic == CDR_VOICE_TOPIC:
            return {
                "msisdn": fields[0],
                "tower_id": fields[1],
                "call_type": fields[2],
                "dest_nr": fields[3],
                "call_duration_sec": fields[4],
                "start_time": fields[5]}
           
    except Exception as e:
        logger.error(f"Failed to deserialize message: {e}")
        return None
    
def validate_message(message, topic):

    if message is None:
        return False 
      
    required_fields = {
        CDR_DATA_TOPIC: ["msisdn", "tower_id", "up_bytes", "down_bytes", "data_type", "ip_address", "website_url", "event_datetime"],
        CDR_VOICE_TOPIC: ["msisdn", "tower_id", "call_type", "dest_nr", "call_duration_sec", "start_time"]
        }
    
    for field in required_fields[topic]:
        if field not in message or not message[field]:
            logger.warning(f"Missing required field: {field}")
            return False
        
    numerical_fields = {
        CDR_DATA_TOPIC: ["up_bytes", "down_bytes","tower_id"],
        CDR_VOICE_TOPIC: ["call_duration_sec","tower_id"]
        }
    for field in numerical_fields[topic]:
        if not message[field].isdigit():
            logger.warning(f"Field {field} is not numeric: {message[field]}")
            return False
        
    return True

def send_to_dlq(producer,topic,message):
    dlq_topic = CDR_DATA_DLQ if topic == CDR_DATA_TOPIC else CDR_VOICE_DLQ
    for _ in range(3):
        try:
            producer.send(dlq_topic, value=json.dumps(message))
            logger.info(f"Sent message to DLQ: {message}")
            return
        except Exception as e:
            logger.error(f"Failed to send to DLQ: {e}")
            time.sleep(1)

def connect_to_producer():
    for i in range(10):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                retries=3
            )
            logger.info("Connected to Kafka Producer")
            return producer
        except Exception as e:
            logger.error(f"Failed to connect to Kafka Producer: {e}")
            time.sleep(1)

def main():

    consumer = connect_to_kafka()
    producer = connect_to_producer()

    for message in consumer:
        topic = message.topic
        deserialized_message = deserialize_message(message.value, topic)
        if validate_message(deserialized_message, topic):
            logger.info(f"Valid message: {deserialized_message}")
        else:
            logger.warning(f"Invalid message: {message.value}")
            send_to_dlq(producer, topic, deserialized_message)

main()