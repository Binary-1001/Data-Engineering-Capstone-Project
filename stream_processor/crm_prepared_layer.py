import logging


# TBD-7
# BUILD CRM PREPARED LAYER


# LOGGER CONFIGURATION
# Creates readable logs to monitor CRM analytics processing

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


# RAW CRM DATA
# Simulates raw CRM records coming from the persistence layer

crm_accounts = [
    {
        "account_id": 101,
        "customer_id": 1,
        "account_type": "Premium"
    }
]

crm_addresses = [
    {
        "customer_id": 1,
        "city": "Johannesburg",
        "province": "Gauteng"
    }
]

crm_devices = [
    {
        "customer_id": 1,
        "device_name": "iPhone 15"
    }
]

crm_customers = [
    {
        "customer_id": 1,
        "full_name": "John Doe"
    }
]


# ANALYTICS OUTPUT STORAGE
# Stores flattened CRM analytics records for reporting

analytics_crm_flat = []


# DUPLICATE TRACKING
# Tracks processed customers to prevent duplicate records

processed_customers = set()


# CRM PREPARED LAYER
# Merges CRM datasets into a flattened analytics-ready view

def build_crm_prepared_layer():

    logger.info("Starting CRM prepared layer processing")


    # ROW PROCESSING METRICS
    # Counts how many records were transformed successfully

    rows_processed = 0


    try:

        # LOOP THROUGH CUSTOMER DATA
        # Processes one CRM customer record at a time

        for customer in crm_customers:

            customer_id = customer["customer_id"]


            # DUPLICATE HANDLING
            # Skips customers already processed previously

            if customer_id in processed_customers:

                logger.warning(
                    f"Duplicate customer skipped: {customer_id}"
                )

                continue

            processed_customers.add(customer_id)


            # ACCOUNT LOOKUP
            # Finds matching customer account information

            account = next(
                (
                    item for item in crm_accounts
                    if item["customer_id"] == customer_id
                ),
                {}
            )


            # ADDRESS LOOKUP
            # Finds matching customer address information

            address = next(
                (
                    item for item in crm_addresses
                    if item["customer_id"] == customer_id
                ),
                {}
            )


            # DEVICE LOOKUP
            # Finds matching customer device information

            device = next(
                (
                    item for item in crm_devices
                    if item["customer_id"] == customer_id
                ),
                {}
            )


            # VALIDATION CHECKS
            # Ensures required customer fields are available

            if not customer.get("full_name"):

                logger.warning(
                    f"Customer validation failed: {customer}"
                )

                continue


            # FLATTENED CUSTOMER VIEW
            # Combines CRM data into a single analytics-ready record

            flattened_record = {

                "customer_id": customer_id,

                "full_name":
                    customer.get("full_name"),

                "account_id":
                    account.get("account_id"),

                "account_type":
                    account.get("account_type"),

                "city":
                    address.get("city"),

                "province":
                    address.get("province"),

                "device_name":
                    device.get("device_name")
            }


            # SAVE TO ANALYTICS LAYER
            # Stores flattened CRM records into analytics storage

            analytics_crm_flat.append(flattened_record)

            rows_processed += 1

            logger.info(
                f"Flattened CRM record created "
                f"for customer {customer_id}"
            )


        # PROCESSING METRICS
        # Displays CRM transformation statistics

        logger.info(
            f"Rows processed successfully: "
            f"{rows_processed}"
        )

        logger.info(
            "CRM prepared layer completed successfully"
        )

        logger.info(
            f"Analytics records created: "
            f"{len(analytics_crm_flat)}"
        )

        return analytics_crm_flat


    # TRANSFORMATION FAILURE HANDLING
    # Handles unexpected transformation errors safely

    except Exception as e:

        logger.error(
            f"CRM prepared layer transformation failed: {e}"
        )

        return []


# APPLICATION ENTRY POINT
# Runs CRM prepared layer independently

if __name__ == "__main__":

    results = build_crm_prepared_layer()

    print("\nFLATTENED CRM ANALYTICS DATASET\n")

    for row in results:
        print(row)