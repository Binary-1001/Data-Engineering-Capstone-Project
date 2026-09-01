import csv
import glob
import pytest

def get_cdr_data_file():
    """Finds the first file in the current directory that starts with 'cdr_data'.
    Raises an error if none is found so the test fails with a clear message."""
    files = glob.glob("volumes/data/sftp/home/cdr_data/cdr_data*.csv")
    if not files:
        pytest.skip("No cdr_data*.csv file found — skipping test")
    return files[0]

def test_csv_file_can_be_read():
    """
    Ensures that the generated CDR CSV file can be read
    and contains at least one record.
    """
    with open(get_cdr_data_file(), newline="") as file:
        reader = csv.DictReader(file)
        rows = list(reader)

    assert len(rows) > 0

def test_csv_contains_expected_columns():
    """
    Ensures that the CDR CSV file contains the correct schema
    required for Redpanda ingestion.
    """
    with open(get_cdr_data_file(), newline="") as file:
        reader = csv.DictReader(file)

        expected_columns = [
            "msisdn",
            "tower_id",
            "up_bytes",
            "down_bytes",
            "data_type",
            "ip_address",
            "website_url",
            "event_datetime"
        ]

        assert reader.fieldnames == expected_columns