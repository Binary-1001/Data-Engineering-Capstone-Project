import logging


# TBD-7
# BUILD FOREX PREPARED LAYER


# LOGGER CONFIGURATION
# Creates readable logs to monitor forex analytics processing

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


# RAW FOREX DATA
# Simulates raw forex streaming records coming from Kafka

forex_data = [

    {
        "currency_pair": "USD/ZAR",
        "exchange_rate": 18.45,
        "timestamp": "2026-05-12 10:00:00"
    },

    {
        "currency_pair": "USD/ZAR",
        "exchange_rate": 18.20,
        "timestamp": "2026-05-12 09:00:00"
    },

    {
        "currency_pair": "EUR/USD",
        "exchange_rate": 1.10,
        "timestamp": "2026-05-12 10:30:00"
    },

    {
        "currency_pair": None,
        "exchange_rate": 99.99,
        "timestamp": "2026-05-12 11:00:00"
    }
]


# ANALYTICS OUTPUT STORAGE
# Stores cleaned forex analytics records ready for reporting

analytics_forex_rates = []


# LATEST RATE TRACKING
# Keeps only the newest exchange rate per currency pair

latest_rates = {}


# FOREX PREPARED LAYER
# Cleans raw forex records and creates analytics-ready output

def build_forex_prepared_layer():

    logger.info("Starting forex prepared layer processing")


    # ROW PROCESSING METRICS
    # Counts how many records were transformed successfully

    rows_processed = 0


    try:

        # LOOP THROUGH RAW FOREX DATA
        # Processes each incoming forex record one at a time

        for record in forex_data:

            currency_pair = record.get("currency_pair")
            exchange_rate = record.get("exchange_rate")
            timestamp = record.get("timestamp")


            # VALIDATION CHECKS
            # Skips records with missing currency pairs or rates

            if not currency_pair or exchange_rate is None:

                logger.warning(
                    f"Invalid forex record skipped: {record}"
                )

                continue


            # LATEST RATE EXTRACTION
            # Keeps the newest exchange rate for each currency pair

            existing_record = latest_rates.get(currency_pair)

            if (
                existing_record is None
                or timestamp > existing_record["timestamp"]
            ):

                latest_rates[currency_pair] = record


        # FLATTEN ANALYTICS DATASET
        # Converts cleaned forex records into analytics-ready format

        for pair, record in latest_rates.items():

            flattened_record = {

                "currency_pair": pair,

                "latest_exchange_rate":
                    record["exchange_rate"],

                "timestamp":
                    record["timestamp"]
            }


            # SAVE TO ANALYTICS LAYER
            # Stores cleaned forex records into analytics storage

            analytics_forex_rates.append(flattened_record)

            rows_processed += 1

            logger.info(
                f"Latest forex rate prepared for {pair}"
            )


        # PROCESSING METRICS
        # Displays how many records were processed successfully

        logger.info(
            f"Rows processed successfully: "
            f"{rows_processed}"
        )

        logger.info(
            "Forex prepared layer completed successfully"
        )

        logger.info(
            f"Analytics forex records created: "
            f"{len(analytics_forex_rates)}"
        )

        return analytics_forex_rates


    # TRANSFORMATION FAILURE HANDLING
    # Handles unexpected transformation errors safely

    except Exception as e:

        logger.error(
            f"Forex prepared layer transformation failed: {e}"
        )

        return []


# APPLICATION ENTRY POINT
# Runs forex prepared layer independently

if __name__ == "__main__":

    results = build_forex_prepared_layer()

    print("\nFOREX ANALYTICS DATASET\n")

    for row in results:
        print(row)