import unittest
from unittest.mock import MagicMock, patch
from datetime import datetime
import sys
import os

# Add parent directory to path so we can import kafka_test
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kafka_test import build_test_payload, connect_to_redpanda, produce_to_redpanda


class TestBuildTestPayload(unittest.TestCase):
    """Tests for build_test_payload() function"""

    def test_payload_has_all_required_fields(self):
        """Payload must contain all CRM account fields"""
        payload = build_test_payload()
        required_fields = [
            'account_id',
            'owner_name',
            'email',
            'phone_number',
            'modified_ts',
            'event_type'
        ]
        for field in required_fields:
            self.assertIn(field, payload, f"Missing field: {field}")

    def test_payload_account_id_is_correct(self):
        """account_id must be 12345"""
        payload = build_test_payload()
        self.assertEqual(payload['account_id'], 12345)

    def test_payload_event_type_is_insert(self):
        """event_type must be INSERT"""
        payload = build_test_payload()
        self.assertEqual(payload['event_type'], 'INSERT')

    def test_payload_email_is_valid(self):
        """email must contain @ symbol"""
        payload = build_test_payload()
        self.assertIn('@', payload['email'])

    def test_payload_modified_ts_is_string(self):
        """modified_ts must be a string timestamp"""
        payload = build_test_payload()
        self.assertIsInstance(payload['modified_ts'], str)

    def test_payload_modified_ts_format(self):
        """modified_ts must match expected datetime format"""
        payload = build_test_payload()
        try:
            datetime.strptime(payload['modified_ts'], '%Y-%m-%d %H:%M:%S.%f')
        except ValueError:
            self.fail("modified_ts format is incorrect")

    def test_payload_is_dict(self):
        """Payload must be a dictionary"""
        payload = build_test_payload()
        self.assertIsInstance(payload, dict)

    def test_payload_phone_number_is_string(self):
        """phone_number must be a string"""
        payload = build_test_payload()
        self.assertIsInstance(payload['phone_number'], str)


class TestConnectToRedpanda(unittest.TestCase):
    """Tests for connect_to_redpanda() function"""

    @patch('kafka_test.KafkaProducer')
    def test_connect_returns_producer(self, mock_kafka):
        """connect_to_redpanda must return a producer object"""
        mock_producer = MagicMock()
        mock_kafka.return_value = mock_producer
        result = connect_to_redpanda()
        self.assertEqual(result, mock_producer)

    @patch('kafka_test.KafkaProducer')
    def test_connect_calls_kafka_producer(self, mock_kafka):
        """KafkaProducer must be called once"""
        connect_to_redpanda()
        mock_kafka.assert_called_once()

    @patch('kafka_test.KafkaProducer')
    def test_connect_uses_correct_servers(self, mock_kafka):
        """Producer must connect to configured KAFKA_SERVERS"""
        connect_to_redpanda()
        call_kwargs = mock_kafka.call_args[1]
        self.assertIn('bootstrap_servers', call_kwargs)

    @patch('kafka_test.KafkaProducer')
    def test_connect_failure_raises_exception(self, mock_kafka):
        """Connection failure must raise an exception"""
        mock_kafka.side_effect = Exception("Connection refused")
        with self.assertRaises(Exception):
            connect_to_redpanda()


class TestProduceToRedpanda(unittest.TestCase):
    """Tests for produce_to_redpanda() function"""

    def test_produce_sends_message_successfully(self):
        """produce_to_redpanda must send message without errors"""
        mock_producer = MagicMock()
        mock_future = MagicMock()
        mock_metadata = MagicMock()
        mock_metadata.topic = 'crm.accounts'
        mock_metadata.partition = 0
        mock_metadata.offset = 0
        mock_future.get.return_value = mock_metadata
        mock_producer.send.return_value = mock_future

        payload = build_test_payload()
        produce_to_redpanda(
            producer=mock_producer,
            topic='crm.accounts',
            key='account-12345',
            record=payload
        )
        mock_producer.send.assert_called_once()

    def test_produce_calls_send_with_correct_topic(self):
        """produce_to_redpanda must send to the correct topic"""
        mock_producer = MagicMock()
        mock_future = MagicMock()
        mock_metadata = MagicMock()
        mock_future.get.return_value = mock_metadata
        mock_producer.send.return_value = mock_future

        payload = build_test_payload()
        produce_to_redpanda(
            producer=mock_producer,
            topic='crm.accounts',
            key='account-12345',
            record=payload
        )
        call_args = mock_producer.send.call_args
        self.assertEqual(call_args[0][0], 'crm.accounts')

    def test_produce_retries_on_failure(self):
        """produce_to_redpanda must retry on KafkaError"""
        from kafka.errors import KafkaError
        mock_producer = MagicMock()
        mock_future = MagicMock()
        mock_future.get.side_effect = KafkaError("Send failed")
        mock_producer.send.return_value = mock_future

        payload = build_test_payload()
        with self.assertRaises(Exception) as context:
            produce_to_redpanda(
                producer=mock_producer,
                topic='crm.accounts',
                key='account-12345',
                record=payload
            )
        self.assertIn('100 attempts', str(context.exception))

    def test_produce_raises_after_max_retries(self):
        """produce_to_redpanda must raise exception after 100 failed attempts"""
        from kafka.errors import KafkaError
        mock_producer = MagicMock()
        mock_future = MagicMock()
        mock_future.get.side_effect = KafkaError("Send failed")
        mock_producer.send.return_value = mock_future

        payload = build_test_payload()
        with self.assertRaises(Exception):
            produce_to_redpanda(
                producer=mock_producer,
                topic='crm.accounts',
                key='account-12345',
                record=payload
            )


if __name__ == '__main__':
    unittest.main(verbosity=2)