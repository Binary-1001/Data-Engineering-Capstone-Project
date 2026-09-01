from cdr.main import generate_cdr_data, generate_cdr_voice
from datetime import datetime

def test_generate_cdr_data_returns_records():
    """
    Verifies that CDR data generation returns the correct number of records
    and outputs a list structure.
    """

    records = generate_cdr_data(datetime.now(), 5)

    assert len(records) == 5
    assert isinstance(records, list)


def test_generate_cdr_data_has_required_fields():
    """
    Ensures that generated CDR data contains all required fields
    for downstream processing in Redpanda.
    """

    records = generate_cdr_data(datetime.now(), 1)
    record = records[0]

    expected_fields = [
        "msisdn",
        "tower_id",
        "up_bytes",
        "down_bytes",
        "data_type",
        "ip_address",
        "website_url",
        "event_datetime"
    ]

    for field in expected_fields:
        assert field in record


def test_generate_cdr_voice_returns_records():
    """
    Verifies that voice CDR generation returns the correct number
    of records in list format.
    """

    records = generate_cdr_voice(datetime.now(), 5)

    assert len(records) == 5


def test_generate_cdr_voice_has_required_fields():
    """
    Ensures that voice CDR records contain all required fields
    for call processing and cost calculation.
    """

    records = generate_cdr_voice(datetime.now(), 1)
    record = records[0]

    expected_fields = [
        "msisdn",
        "tower_id",
        "call_type",
        "dest_nr",
        "call_duration_sec",
        "start_time"
    ]

    for field in expected_fields:
        assert field in record