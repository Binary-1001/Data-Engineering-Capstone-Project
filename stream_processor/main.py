import json
import os
import time
import logging
from kafka import KafkaConsumer
from sqlalchemy import create_engine, text
from aggregator import aggregate_data_event, aggregate_voice_event
from crm_prepared_layer import build_crm_prepared_layer
from forex_prepared_layer import build_forex_prepared_layer


# LOGGER CONFIGURATION
# Formats runtime logs with timestamps and log levels

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)


# ENVIRONMENT DETECTION
# Uses localhost in development and Docker hostnames in production

environment = 'dev' if os.getenv('USER', '') != '' else 'prod'


# KAFKA CONFIGURATION
# Connects to Redpanda brokers

KAFKA_SERVERS = (
    ['localhost:19092', 'localhost:29092', 'localhost:39092']
    if environment == 'dev'
    else ['redpanda-0:9092', 'redpanda-1:9092', 'redpanda-2:9092']
)


# DATABASE CONFIGURATION
# Connects to analytics PostgreSQL database

DB_URL = (
    'postgresql://postgres:postgres@localhost:15432/wtc_analytics'
    if environment == 'dev'
    else 'postgresql://postgres:postgres@postgres:5432/wtc_analytics'
)


# FLUSH INTERVAL
# Writes aggregated data every 30 seconds

FLUSH_INTERVAL_SEC = 30


# STREAM TOPICS
# Topics consumed from Redpanda

CDR_DATA_TOPIC = 'cdr-data'
CDR_VOICE_TOPIC = 'cdr-voice'


# STREAM PROCESSING METRICS
# Tracks runtime statistics

metrics = {

    "messages_processed": 0,
    "messages_errored": 0,
    "flushes_completed": 0,
    "flush_errors": 0,
    "total_flush_duration_sec": 0.0
}


# DATA UPSERT SQL
# Aggregates daily data usage into prepared layer

UPSERT_DATA_SQL = text("""

    INSERT INTO prepared_layers.cdr_daily_data_summary
        (msisdn, date, usage_type, up_bytes, down_bytes, cost_wak, updated_at)

    VALUES
        (:msisdn, :date, :usage_type,
         :up_bytes, :down_bytes, :cost_wak, NOW())

    ON CONFLICT (msisdn, date, usage_type)

    DO UPDATE SET

        up_bytes   =
            prepared_layers.cdr_daily_data_summary.up_bytes
            + EXCLUDED.up_bytes,

        down_bytes =
            prepared_layers.cdr_daily_data_summary.down_bytes
            + EXCLUDED.down_bytes,

        cost_wak   =
            prepared_layers.cdr_daily_data_summary.cost_wak
            + EXCLUDED.cost_wak,

        updated_at = NOW()
""")


# VOICE UPSERT SQL
# Aggregates daily voice usage into prepared layer

UPSERT_VOICE_SQL = text("""

    INSERT INTO prepared_layers.cdr_daily_voice_summary
        (msisdn, date, usage_type,
         call_duration_sec, cost_wak, updated_at)

    VALUES
        (:msisdn, :date, :usage_type,
         :call_duration_sec, :cost_wak, NOW())

    ON CONFLICT (msisdn, date, usage_type)

    DO UPDATE SET

        call_duration_sec =
            prepared_layers.cdr_daily_voice_summary.call_duration_sec
            + EXCLUDED.call_duration_sec,

        cost_wak =
            prepared_layers.cdr_daily_voice_summary.cost_wak
            + EXCLUDED.cost_wak,

        updated_at = NOW()
""")


# FLUSH STATE FUNCTION
# Writes aggregated memory state into PostgreSQL

def flush_state(engine, data_state: dict, voice_state: dict):

    # skip if nothing to write

    if not data_state and not voice_state:
        return

    flush_start = time.time()

    try:

        with engine.begin() as conn:

            # DATA SUMMARY FLUSH
            # Writes aggregated data usage metrics

            if data_state:

                conn.execute(
                    UPSERT_DATA_SQL,
                    list(data_state.values())
                )

                logger.info(
                    f"Wrote {len(data_state)} "
                    f"data usage summaries to Postgres"
                )

            # VOICE SUMMARY FLUSH
            # Writes aggregated voice usage metrics

            if voice_state:

                conn.execute(
                    UPSERT_VOICE_SQL,
                    list(voice_state.values())
                )

                logger.info(
                    f"Wrote {len(voice_state)} "
                    f"voice usage summaries to Postgres"
                )

        # MEMORY CLEANUP
        # Clears in-memory aggregation state

        data_state.clear()
        voice_state.clear()

        # FLUSH METRICS
        # Tracks flush performance statistics

        duration = time.time() - flush_start

        metrics["flushes_completed"] += 1
        metrics["total_flush_duration_sec"] += duration

        avg_flush = (
            metrics["total_flush_duration_sec"]
            / metrics["flushes_completed"]
        )

        logger.info(
            f"[METRICS] flush_duration={duration:.3f}s "
            f"avg_flush={avg_flush:.3f}s "
            f"total_flushes={metrics['flushes_completed']}"
        )

    except Exception as e:

        metrics["flush_errors"] += 1

        logger.error(
            f"Failed to write to Postgres "
            f"(total_flush_errors={metrics['flush_errors']}): {e}"
        )


# MAIN STREAM PROCESSOR
# Handles Kafka consumption and analytics processing

def main():

    # POSTGRES CONNECTION
    # Connects to analytics database

    engine = create_engine(DB_URL)

    logger.info(f"Connected to Postgres at {DB_URL}")


    # KAFKA CONSUMER
    # Subscribes to Redpanda topics

    consumer = KafkaConsumer(

        CDR_DATA_TOPIC,
        CDR_VOICE_TOPIC,

        bootstrap_servers=KAFKA_SERVERS,

        group_id='cdr-stream-processor',

        auto_offset_reset='earliest',

        enable_auto_commit=True,

        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )


    # AGGREGATION STATE
    # Stores running aggregation totals

    data_state = {}
    voice_state = {}

    last_flush = time.time()


    # STREAM STARTUP LOGGING

    logger.info(
        f"Stream processor started — listening on topics: "
        f"{CDR_DATA_TOPIC}, {CDR_VOICE_TOPIC}"
    )

    logger.info(
        f"Will flush aggregated data to Postgres every "
        f"{FLUSH_INTERVAL_SEC} seconds"
    )


    # CRM PREPARED LAYER
    # Builds flattened CRM analytics dataset

    crm_analytics_data = build_crm_prepared_layer()

    logger.info(
        f"CRM analytics records prepared: "
        f"{len(crm_analytics_data)}"
    )


    # FOREX PREPARED LAYER
    # Builds cleaned forex analytics dataset

    forex_analytics_data = build_forex_prepared_layer()

    logger.info(
        f"Forex analytics records prepared: "
        f"{len(forex_analytics_data)}"
    )


    # DISPATCH TABLE
    # Maps Kafka topics to aggregation handlers

    handlers = {

        CDR_DATA_TOPIC:
            (aggregate_data_event, data_state),

        CDR_VOICE_TOPIC:
            (aggregate_voice_event, voice_state),
    }

    try:

        while True:

            # KAFKA POLLING
            # Reads streaming records from Redpanda

            records = consumer.poll(timeout_ms=1000)

            for tp, messages in records.items():

                handler, state = handlers.get(tp.topic, (None, None))

                for msg in messages:

                    try:

                        # EVENT PROCESSING
                        # Executes aggregation handler

                        if handler:
                            handler(state, msg.value)

                        metrics["messages_processed"] += 1

                    except Exception as e:

                        metrics["messages_errored"] += 1

                        logger.error(
                            f"Could not process message "
                            f"(total_errors="
                            f"{metrics['messages_errored']}): {e}"
                        )

            # FLUSH CHECK
            # Writes data to PostgreSQL every interval

            if time.time() - last_flush >= FLUSH_INTERVAL_SEC:

                flush_state(engine, data_state, voice_state)

                last_flush = time.time()

                logger.info(
                    f"[METRICS] "
                    f"messages_processed="
                    f"{metrics['messages_processed']} "
                    f"messages_errored="
                    f"{metrics['messages_errored']} "
                    f"flush_errors="
                    f"{metrics['flush_errors']}"
                )

    except KeyboardInterrupt:

        logger.info(
            "Shutdown signal received — "
            "flushing remaining data before exit..."
        )

    finally:

        # FINAL FLUSH
        # Ensures remaining data is persisted

        flush_state(engine, data_state, voice_state)

        consumer.close()

        logger.info("Stream processor stopped cleanly")


# APPLICATION ENTRY POINT
# Starts stream processor

if __name__ == '__main__':
    main()