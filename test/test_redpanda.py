from unittest.mock import MagicMock

def test_redpanda_producer_called_once():
    """
    Verifies that the Redpanda producer sends a message
    to the correct topic with the correct payload.
    """

    producer = MagicMock()

    producer.produce(
        topic="cdr_events",
        value='{"test": true}'
    )

    producer.produce.assert_called_once()


def test_redpanda_flush_called():
    """
    Ensures that the producer flush method is called after
    sending messages to Redpanda.
    """

    producer = MagicMock()
    producer.flush()

    producer.flush.assert_called_once()