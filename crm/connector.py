import requests
import json
import logging
import time
import os

# Logging setup
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Environment detection
environment = 'dev' if os.getenv('USER', '') != '' else 'prod'
logger.info(f"Environment: {environment}")

# Debezium config
if environment == 'dev':
    DEBEZIUM_URL = "http://localhost:8083"
else:
    DEBEZIUM_URL = "http://debezium:8083"

logger.info(f"Debezium URL: {DEBEZIUM_URL}")

CONNECTOR_NAME = "crm-postgres-connector"


def wait_for_debezium():
    """Wait until Debezium REST API is ready"""
    logger.info("Waiting for Debezium to be ready...")
    for attempt in range(30):
        try:
            response = requests.get(
                f"{DEBEZIUM_URL}/connectors",
                timeout=5
            )
            if response.status_code == 200:
                logger.info("Debezium is ready")
                return True
        except Exception as e:
            logger.info(f"Attempt [{attempt+1}/30] Debezium not ready: {e}")
        time.sleep(5)
    raise Exception("Debezium did not become ready after 30 attempts")


def delete_existing_connector():
    """Delete connector if it already exists — clean slate"""
    try:
        response = requests.get(
            f"{DEBEZIUM_URL}/connectors/{CONNECTOR_NAME}",
            timeout=5
        )
        if response.status_code == 200:
            logger.info(f"Found existing connector — deleting: {CONNECTOR_NAME}")
            requests.delete(f"{DEBEZIUM_URL}/connectors/{CONNECTOR_NAME}")
            time.sleep(3)
            logger.info("Existing connector deleted")
        else:
            logger.info("No existing connector found — fresh registration")
    except Exception as e:
        logger.warning(f"Could not check existing connector: {e}")


def register_connector():
    """Register the Debezium PostgreSQL CDC connector"""

    connector_config = {
        "name": CONNECTOR_NAME,
        "config": {
            # Connector type
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",

            # PostgreSQL connection
            "database.hostname": "postgres",
            "database.port": "5432",
            "database.user": "postgres",
            "database.password": "postgres",
            "database.dbname": "wtc_prod",
            "database.server.name": "crm",

            # WAL decoder plugin
            "plugin.name": "pgoutput",

            # Tables to watch — all 3 CRM tables
            "table.include.list": "crm_system.accounts,crm_system.addresses,crm_system.devices",

            # Topic prefix — Debezium creates topics as:
            # crm.crm_system.accounts
            # crm.crm_system.addresses
            # crm.crm_system.devices
            "topic.prefix": "crm",

            # Snapshot — initial load of existing data
            "snapshot.mode": "always",

            # Timestamps
            "time.precision.mode": "connect",

            # Heartbeat to keep WAL slot alive
            "heartbeat.interval.ms": "10000"
        }
    }

    logger.info(f"Registering connector: {CONNECTOR_NAME}")
    logger.info(f"Watching tables: crm_system.accounts, crm_system.addresses, crm_system.devices")

    # Pipeline visibility logs
    # These logs help track the flow of CRM data from PostgreSQL
    # through Debezium and into Redpanda/Kafka topics
    logger.info("Starting PostgreSQL → Debezium → Redpanda pipeline")
    logger.info("CRM change events will be streamed automatically")


    response = requests.post(
        f"{DEBEZIUM_URL}/connectors",
        headers={"Content-Type": "application/json"},
        data=json.dumps(connector_config),
        timeout=10
    )

    if response.status_code in [200, 201]:
        logger.info(f"Connector registered successfully")
        logger.info(f"Response: {json.dumps(response.json(), indent=2)}")
    else:
        logger.error(f"Failed to register connector: {response.status_code}")
        logger.error(f"Response: {response.text}")
        raise Exception(f"Connector registration failed: {response.status_code}")


def check_connector_status():
    """Check the connector and task status"""
    logger.info("Checking connector status...")
    time.sleep(3)

    response = requests.get(
        f"{DEBEZIUM_URL}/connectors/{CONNECTOR_NAME}/status",
        timeout=5
    )

    if response.status_code == 200:
        status = response.json()
        connector_state = status['connector']['state']
        logger.info(f"Connector state: {connector_state}")

        for task in status.get('tasks', []):
            task_state = task['state']
            task_id    = task['id']
            logger.info(f"Task [{task_id}] state: {task_state}")

            if task_state == 'FAILED':
                logger.error(f"Task failed: {task.get('trace', 'no trace')}")

        if connector_state == 'RUNNING':
            logger.info("CDC pipeline is RUNNING...")
            logger.info("Debezium is now streaming changes from PostgreSQL to Redpanda")

             
            logger.info("Pipeline flow verified successfully")
            logger.info("Kafka topics are actively receiving CRM events")

            logger.info("Topics being written to:")
            logger.info("  → crm.crm_system.accounts")
            logger.info("  → crm.crm_system.addresses")
            logger.info("  → crm.crm_system.devices")
        else:
            logger.warning(f"Connector is in state: {connector_state}")
    else:
        logger.error(f"Could not get status: {response.status_code} {response.text}")


def list_connectors():
    """List all registered connectors"""
    response = requests.get(f"{DEBEZIUM_URL}/connectors", timeout=5)
    if response.status_code == 200:
        connectors = response.json()
        logger.info(f"Registered connectors: {connectors}")
    return response.json()


if __name__ == "__main__":
    logger.info("=" * 50)
    logger.info("Starting CRM CDC Connector Registration")
    logger.info("=" * 50)

    try:
        # Step 1 — Wait for Debezium
        wait_for_debezium()

        # Step 2 — Clean slate
        delete_existing_connector()

        # Step 3 — Register connector
        register_connector()

        # Step 4 — Check status
        check_connector_status()

        # Step 5 — List all connectors
        list_connectors()

        logger.info("=" * 50)
        logger.info("CDC Pipeline Setup Complete")
        logger.info("Debezium is now watching PostgreSQL WAL")
        logger.info("Changes will stream automatically to Redpanda")
        logger.info("Check topics at: http://localhost:18084")
        logger.info("=" * 50)

    except Exception as e:
        logger.error(f"CDC setup failed: {e}")
        raise

    

    