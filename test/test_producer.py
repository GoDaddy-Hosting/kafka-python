import unittest
from unittest.mock import MagicMock
from kafka.producer import Producer

class TestProducer(unittest.TestCase):

    def setUp(self):
        self.client = MagicMock()
        self.producer = Producer(self.client)

    def test_construct_producer(self):
        self.assertIsInstance(self.producer, Producer)

    def test_ack_timeout_setter_getter(self):
        self.producer.ack_timeout = 100
        self.assertEqual(self.producer.ack_timeout, 100)

    def test_ack_timeout_exception(self):
        with self.assertRaises(ValueError) as ex:
            self.producer.ack_timeout = -11

    def test_require_ack(self):
        self.producer.require_ack = Producer.ACK_AFTER_CLUSTER_COMMIT
        self.assertEqual(self.producer.require_ack, Producer.ACK_AFTER_CLUSTER_COMMIT)

    def test_require_ack_exception(self):
        with self.assertRaises(ValueError):
            self.producer.require_ack = 10

    def test_send_messages_client_args(self):
        topic, partition, msg = 'bradrocks', 1, 'message one'
        r = self.producer.send_messages(topic, partition, msg)
        expectedArgs = {'acks':self.producer.require_ack, 'timeout':self.producer.ack_timeout}
        self.assertEqual(self.client.send_produce_request.call_args[1], expectedArgs)

    def test_send_messages_client_response(self):
        expectedResp = 'got it!'
        self.client.send_produce_request.return_value = expectedResp
        r = self.producer.send_messages(None, None, None)
        self.assertEqual(expectedResp, r)

    def test_send_messages_client_exception(self):
        self.client.send_produce_request.side_effect = Exception('hey!')
        with self.assertRaises(Exception):
            self.producer.send_messages(None, None, None)

if __name__ == '__main__':
    unittest.main()
