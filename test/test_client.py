import unittest
from unittest.mock import MagicMock, patch
from kafka.client import KafkaClient
from kafka.common import (BrokerMetadata, FetchRequest, FetchResponse, PartitionMetadata, ProduceRequest, ProduceResponse)
from kafka.protocol import KafkaProtocol, create_message

class TestClient(unittest.TestCase):

    def test_client_init(self):
        client = KafkaClient("localhost:9092", load_hosts=False, load_metadata=False)
        self.assertIsNotNone(client)

    def test_client_init_collects_hosts(self):
        fake_hosts = ['host1','host2']
        client = None
        with patch('kafka.client.collect_hosts', return_value=fake_hosts) as mock_method:
            client = KafkaClient("localhost:9092", load_metadata=False)

        self.assertTrue(mock_method.called)
        self.assertListEqual(client.hosts, fake_hosts)

    def test_load_metadata_for_topics(self):
        client = KafkaClient("localhost:9092", load_hosts=False, load_metadata=False)
        broker_data = {0: BrokerMetadata(nodeId=0, host=b'localhost', port=9092)}
        topic_data = {b'test_topic': {0: PartitionMetadata(topic=b'test_topic', partition=0, leader=0, replicas=(0,), isr=(0,))}}
        client.request_metadata = MagicMock(return_value=(broker_data, topic_data))

        client.load_metadata_for_topics('test_topic')
        self.assertTrue(client.request_metadata.called)

    def test_request_metadata(self):
        client = KafkaClient("192.168.101.11:9092", load_hosts=False, load_metadata=False)

        encoded_metadata_request = b'\x00\x00\x00&\x00\x03\x00\x00\x00\x00\x00\x00\x00\x0ckafka-python\x00\x00\x00\x01\x00\ntest_topic'
        KafkaProtocol.encode_metadata_request = MagicMock(return_value=encoded_metadata_request)

        encoded_metadata_response = (b'\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x0e192.168.101.11'
                                     b'\x00\x00#\x84\x00\x00\x00\x02\x00\x0e192.168.101.11\x00\x00#\x86\x00'
                                     b'\x00\x00\x01\x00\x00\x00\ntest_topic\x00\x00\x00\x02\x00\x00\x00\x00'
                                     b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00'
                                     b'\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00'
                                     b'\x00\x00\x02\x00\x00\x00\x01\x00\x00\x00\x02\x00\x00\x00\x01\x00\x00\x00\x02')
        client._send_broker_unaware_request = MagicMock(return_value=encoded_metadata_response)

        brokers, topics = client.request_metadata("test_topic")
        self.assertIsNotNone(brokers)
        self.assertIsNotNone(topics)

    def test_send_produce_request(self):
        client = KafkaClient("localhost:9092", load_hosts=False, load_metadata=False)

        broker_response = ProduceResponse(topic=b'test_topic', partition=0, error=0, offset=2)
        client._send_broker_aware_request = MagicMock(return_value=[broker_response])

        req = ProduceRequest(b"test_topic", 0, [create_message(b"test")])
        out = client.send_produce_request(payloads=[req])
        self.assertListEqual(out, [broker_response])

    def test_send_fetch_request(self):
        client = KafkaClient("localhost:9092", load_hosts=False, load_metadata=False)

        request = FetchRequest(b"test_topic", 0, 0, 4096)
        response = FetchResponse(topic=b'test_topic', partition=0, error=0, highwaterMark=7, messages=[create_message(b"test")])
        client._send_broker_aware_request = MagicMock(return_value=[response])

        out = client.send_fetch_request([request])
        self.assertListEqual(out, [response])


if __name__ == '__main__':
    unittest.main()
