import os
import time
import logging
import paramiko
import csv
from pathlib import Path
from paramiko import SSHClient
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',datefmt='%Y-%m-%d %H:%M:%S ')
logging.getLogger('paramiko').setLevel(level=logging.WARN)
logger = logging.getLogger(__name__)


environment = 'dev' if os.getenv('USER', '') != '' else 'prod'

if environment == 'dev':
    SFTP_HOSTNAME = "localhost"
    SFTP_PORT = 10022
    STAGING_DIR = Path("/home/blessing/data-engineering-project-2024/sftp_consumer/staging")
    KAFKA_SERVERS=['localhost:19092','localhost:29092','localhost:39092','localhost:49092','localhost:59092']
    
else:
    SFTP_HOSTNAME = "sftp"
    SFTP_PORT = 22
    STAGING_DIR = Path("/app/staging")
    KAFKA_SERVERS=['redpanda-0:9092','redpanda-1:9092','redpanda-2:9092']

CDR_DATA_TOPIC = 'cdr-data'
CDR_VOICE_TOPIC = 'cdr-voice'

SFTP_USERNAME = "cdr_data"
SFTP_PASSWORD = "password"
SLEEP_TIME = 30

STAGING_DIR.mkdir(parents=True, exist_ok=True)


def connect_to_sftp(): #Establishing a connection to the sftp

    while True:
        try:
            ssh = SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(hostname=SFTP_HOSTNAME, port=SFTP_PORT, username=SFTP_USERNAME, password=SFTP_PASSWORD, disabled_algorithms={'keys': ['rsa-sha2-256', 'rsa-sha2-512']})
            sftp = ssh.open_sftp()
            logger.info("Connected to SFTP server")
            return sftp
        except Exception as e:
            logger.error(f"An error occurred: {e}")
            time.sleep(1)

def list_files_on_sftp(sftp): #Listing all the files on the sftp

    try:
        files = sftp.listdir(path='/') # Path being the upload directory on the sftp server
        logger.info(f"Files on SFTP server: {files}")
        return files
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return []
    
def download_files(sftp,filename):
    try:
        local_path = STAGING_DIR / filename # Staging directory is the local directory where the files will be downloaded
        remote_path = f"/{filename}"
        sftp.get(remote_path, str(local_path)) # Downloading the files from the sftp server to the local directory
        logger.info(f"Downloaded file: {filename} to {local_path}")
        return local_path
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return None
    
    
def connect_to_kafka():
    producer = KafkaProducer(bootstrap_servers=KAFKA_SERVERS,
                             key_serializer=lambda v: v.encode('utf-8'),
                             value_serializer=lambda v: v.encode('utf-8'))
    logger.info("Producing to Kafka")
    return producer

def produce_to_kafka(producer, topic, key, value):

    for _ in range(100):
        try:
            future = producer.send(topic, key=key, value=value)
            producer.flush(timeout=10)
            logger.info(f"Produced to Kafka topic: {topic}")
            return future
        except Exception as e:
            logger.error(f"An error occurred: {e}")
            time.sleep(0.5)
            
    
def main():
    logger.info('Sleeping 30secs to wait for CDR files to be uploaded...')
    time.sleep(30)
    logger.info("Starting main process")
    downloaded_files = []
    producer = connect_to_kafka()

    sftp = connect_to_sftp()

    while True:
        files = list_files_on_sftp(sftp)
        for filename in files:
            if not filename.endswith('.csv'): # Making sure we only dealing with flat file csv
                continue
            if filename not in downloaded_files:
                local_file_path = download_files(sftp, filename)
                if local_file_path:  
                    downloaded_files.append(filename)
                    topic = CDR_DATA_TOPIC if filename.startswith('cdr_data') else CDR_VOICE_TOPIC
                    with open(local_file_path, 'r') as f:
                        reader = csv.DictReader(f)
                        for row in reader:
                            key = filename
                            value = ','.join(row.values())
                            produce_to_kafka(producer, topic, key, value)
                    logger.info(f"Published file: {filename} to topic: {topic}")

        time.sleep(SLEEP_TIME)

 

    
main()