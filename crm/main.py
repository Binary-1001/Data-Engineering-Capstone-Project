from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from faker import Faker
from faker.providers import phone_number
from faker.providers import file
import random
import time
from datetime import datetime, timedelta
from pathlib import PosixPath
import logging
import os

# LOGGER SETUP
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S '
)


# TRANSFORM FUNCTION (

def transform_row(row):
    """
    Converts a raw database row (tuple) into a structured dictionary.
    Example:
    ("2712345678", "John", "iPhone", "Joburg")
    ->
    {
        "msisdn": "...",
        "name": "...",
        "device": "...",
        "location": "..."
    }
    """
    return {
        "msisdn": row[0] if len(row) > 0 else None,
        "name": row[1] if len(row) > 1 else None,
        "device": row[2] if len(row) > 2 else None,
        "location": row[3] if len(row) > 3 else None
    }


# ENVIRONMENT CONFIG

environment = 'dev' if os.getenv('USER', '') != '' else 'prod'

if environment == 'dev':
    DATABASE_URL = "postgresql://postgres:postgres@localhost:15432/wtc_prod"
else:
    DATABASE_URL = "postgresql://postgres:postgres@postgres:5432/wtc_prod"


# PIPELINE CONFIG

TOTAL_SECONDS = 86400
TOTAL_TRANSACTIONS = 5000

INTERVAL_TIME_SEC = round((TOTAL_SECONDS / TOTAL_TRANSACTIONS), 0) / 3

if INTERVAL_TIME_SEC == 0:
    raise Exception("INTERVAL_TIME_SEC can not be 0")

MAX_ACCOUNT_ID = 99999


# HELPER FUNCTIONS

def read_last_idx():
    idx_file = PosixPath('idx_data.dat')
    if not idx_file.exists():
        return 0
    with open(idx_file, mode='+r') as f:
        return int(f.readlines()[0])


def store_idx(idx):
    with open("idx_data.dat", mode='+w') as f:
        f.write(f"{idx}\n")


def gen_account_sql(fake, account_id, file_datetime):
    owner_name = fake.name().replace("'", "")
    email = fake.email().replace("'", "")
    phone_number = fake.msisdn()

    return f"""
    INSERT INTO crm_system.accounts (account_id, owner_name, email, phone_number, modified_ts)
    VALUES ({account_id}, '{owner_name}', '{email}', '{phone_number}', '{file_datetime}')
    ON CONFLICT (account_id) DO UPDATE
    SET owner_name = EXCLUDED.owner_name,
        email = EXCLUDED.email,
        phone_number = EXCLUDED.phone_number,
        modified_ts = EXCLUDED.modified_ts;
    """


# MAIN EXECUTION BLOCK
# -----------------------------
# This ensures database + loop logic only runs when script is executed directly
if __name__ == "__main__":

    logger.info('Waiting for database...')
    time.sleep(5)

    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()

    fake = Faker()
    fake.add_provider(phone_number)
    fake.add_provider(file)

    Faker.seed(418001)
    random.seed(27418001)

    modified_datetime = datetime(2024, 1, 1, 0, 0, 0)
    last_idx = read_last_idx()

    STARTING = True
    event_counter = 0

    for idx in range(TOTAL_TRANSACTIONS):
        run_active = (not idx < last_idx)

        if run_active and STARTING:
            logger.info(f'Starting at idx: {idx}')
            STARTING = False

        with engine.connect() as connection:
            account_id = random.randint(10000, MAX_ACCOUNT_ID)

            modified_datetime += timedelta(seconds=INTERVAL_TIME_SEC)

            sql = gen_account_sql(fake, account_id, modified_datetime)

            if run_active:
                connection.execute(text(sql))
                connection.commit()
                event_counter += 1

        store_idx(idx)

    logger.info(f"Completed generation [{event_counter}]")
    os.unlink('idx_data.dat')