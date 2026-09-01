import logging
import os
import time
from datetime import datetime
from sqlalchemy import create_engine, text

# ================================================================
# LOGGER 
# ================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ================================================================
# ENVIRONMENT DETECTION — same pattern as main.py
# dev = running locally, prod = inside Docker
# ================================================================
environment = 'dev' if os.getenv('USER', '') != '' else 'prod'

# ================================================================
# DATABASE CONFIG
# Points to telecom_platform where analytics tables live
# ================================================================
DB_URL = (
    'postgresql://postgres:postgres@localhost:25432/telecom_platform'
    if environment == 'dev'
    else 'postgresql://postgres:postgres@postgres_persistence:5432/telecom_platform'
)

# ================================================================
# SQL: READ raw CDR events
# Only fetches rows where msisdn and timestamp are present
# Invalid records (nulls) are excluded at the query level
# ================================================================
READ_CDR_SQL = text("""
    SELECT
        msisdn,
        timestamp::date     AS event_date,
        call_duration,
        data_usage_mb,
        cell_tower,
        timestamp
    FROM cdr_raw.cdr_events
    WHERE msisdn IS NOT NULL
      AND timestamp IS NOT NULL
""")

# ================================================================
# SQL: UPSERT aggregated CDR summary
# ON CONFLICT means safe to re-run — existing rows get updated
# not duplicated. Matches the same pattern as main.py's upserts.
# ================================================================
UPSERT_CDR_SQL = text("""
    INSERT INTO analytics.analytics_cdr_summary
        (msisdn, event_date, total_calls, total_duration_sec,
         total_data_mb, last_cell_tower, first_seen, last_seen, updated_at)
    VALUES
        (:msisdn, :event_date, :total_calls, :total_duration_sec,
         :total_data_mb, :last_cell_tower, :first_seen, :last_seen, NOW())
    ON CONFLICT (msisdn, event_date) DO UPDATE SET
        total_calls        = EXCLUDED.total_calls,
        total_duration_sec = EXCLUDED.total_duration_sec,
        total_data_mb      = EXCLUDED.total_data_mb,
        last_cell_tower    = EXCLUDED.last_cell_tower,
        first_seen         = EXCLUDED.first_seen,
        last_seen          = EXCLUDED.last_seen,
        updated_at         = NOW()
""")


# ================================================================
# STEP 1 — READ
# Fetches all valid raw CDR rows from cdr_raw.cdr_events
# ================================================================
def read_raw_cdr(conn):
    logger.info("Reading raw CDR events from cdr_raw.cdr_events")
    result = conn.execute(READ_CDR_SQL)
    rows = result.fetchall()
    logger.info(f"Fetched {len(rows)} valid CDR records")
    return rows


# ================================================================
# STEP 2 — AGGREGATE
# Groups by (msisdn, event_date) in Python
# Same bucketing pattern as aggregator.py's state dict approach
# Skips rows with negative call_duration or data_usage_mb
# ================================================================
def aggregate_cdr(rows):
    logger.info("Aggregating CDR records by msisdn and date")

    state = {}        # key: (msisdn, event_date)
    invalid_count = 0

    for row in rows:
        msisdn, event_date, call_duration, data_usage_mb, cell_tower, timestamp = row

        # INVALID RECORD HANDLING
        # Negative values are data quality issues — skip and count them
        if call_duration is not None and call_duration < 0:
            logger.warning(f"Invalid call_duration {call_duration} for {msisdn} — skipping")
            invalid_count += 1
            continue

        if data_usage_mb is not None and data_usage_mb < 0:
            logger.warning(f"Invalid data_usage_mb {data_usage_mb} for {msisdn} — skipping")
            invalid_count += 1
            continue

        key = (msisdn, event_date)

        # Same setdefault pattern as aggregator.py
        entry = state.setdefault(key, {
            'msisdn':             msisdn,
            'event_date':         event_date,
            'total_calls':        0,
            'total_duration_sec': 0,
            'total_data_mb':      0.0,
            'last_cell_tower':    None,
            'first_seen':         timestamp,
            'last_seen':          timestamp,
        })

        entry['total_calls']        += 1
        entry['total_duration_sec'] += call_duration or 0
        entry['total_data_mb']      += float(data_usage_mb or 0)
        entry['last_cell_tower']     = cell_tower
        entry['last_seen']           = max(entry['last_seen'], timestamp)
        entry['first_seen']          = min(entry['first_seen'], timestamp)

    logger.info(f"Aggregated into {len(state)} summary rows")
    logger.info(f"Invalid records skipped: {invalid_count}")
    return state


# ================================================================
# STEP 3 — FLUSH (write to analytics table)
# Same flush pattern as main.py's flush_state()
# Rounds total_data_mb to 2 decimal places before writing
# ================================================================
def flush_cdr_summary(conn, state):
    if not state:
        logger.info("No CDR data to flush — skipping")
        return

    flush_start = time.time()

    rows = [
        {**entry, 'total_data_mb': round(entry['total_data_mb'], 2)}
        for entry in state.values()
    ]

    conn.execute(UPSERT_CDR_SQL, rows)

    duration = time.time() - flush_start
    logger.info(f"Wrote {len(rows)} CDR summary rows to analytics.analytics_cdr_summary")
    logger.info(f"Flush completed in {duration:.3f}s")


# ================================================================
# MAIN
# ================================================================
def run_cdr_transformer():
    logger.info("Starting CDR transformer")
    start_time = datetime.utcnow()

    engine = create_engine(DB_URL)
    logger.info(f"Connected to database at {DB_URL}")

    try:
        with engine.begin() as conn:
            rows  = read_raw_cdr(conn)
            state = aggregate_cdr(rows)
            flush_cdr_summary(conn, state)

        logger.info("CDR transformation completed successfully")

    except Exception as e:
        logger.error(f"CDR transformation failed: {e}")
        raise

    finally:
        duration = (datetime.utcnow() - start_time).total_seconds()
        logger.info(f"Processing completed in {duration:.2f} seconds")


if __name__ == "__main__":
    run_cdr_transformer()