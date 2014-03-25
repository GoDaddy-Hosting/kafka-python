import unittest
from unittest.mock import MagicMock
from kafka.producer import Producer
from kafka.keyed_producer import KeyedProducer

class TestKeyedProducer(unittest.TestCase):

    def setUp(self):
        self.client = MagicMock()
        self.producer = Producer(self.client)
        self.keyed_producer = KeyedProducer(self.producer)
        self.keyed_producer.partition_cycles = {'provisioning': [1], 'sales': [2]}.items()
        self.keyed_producer._next_partition = MagicMock(return_value=1)

    def test_send_message(self):
        self.producer.send_messages = MagicMock()
        self.keyed_producer.send_message('provisioning', '12345', 'test')
        self.producer.send_messages.assert_called_with('provisioning', 1, 'test')
