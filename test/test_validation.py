from cdr.main import generate_cdr_data
from datetime import datetime

def test_up_bytes_positive():
    """
    Ensures that all generated CDR records have positive upload byte values.
    This validates realistic network usage data.
    """

    records = generate_cdr_data(datetime.now(), 10)
    for record in records:
        assert record["up_bytes"] > 0


def test_down_bytes_positive():
    """
    Ensures that all generated CDR records have positive download byte values.
    This validates realistic network usage data.
    """

    records = generate_cdr_data(datetime.now(), 10)
    for record in records:
        assert record["down_bytes"] > 0


def test_tower_id_range():
    """
    Ensures that tower IDs are within the valid range (1–2000),
    representing realistic telecom tower distribution.
    """

    records = generate_cdr_data(datetime.now(), 10)
    for record in records:
        assert 1 <= record["tower_id"] <= 2000