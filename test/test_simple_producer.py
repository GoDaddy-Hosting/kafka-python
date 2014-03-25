import unittest
from unittest.mock import MagicMock
from kafka.producer import Producer
from kafka.simple_producer import SimpleProducer

class TestSimpleProducer(unittest.TestCase):

    def setUp(self):
        self.client = MagicMock()
        self.producer = Producer(self.client)
        self.simple_producer = SimpleProducer(self.producer)
        self.simple_producer.partition_cycles = {'provisioning': [1], 'sales': [2]}.items()
        self.simple_producer._next_partition = MagicMock(return_value=1)

    def test_send_message_to_first_partition(self):
        self.producer.send_messages = MagicMock()
        self.simple_producer.send_messages('provisioning', 'test')
        self.producer.send_messages.assert_called_with('provisioning', 1, 'test')



