import os
import sys
if sys.version > '3':
    long = int

import random
import struct
import unittest

try:
    from mock import MagicMock, patch
except ImportError:
    from unittest.mock import MagicMock, patch


from kafka import KafkaClient
from kafka.common import (
    ProduceRequest, FetchRequest, Message, ChecksumError,
    ConsumerFetchSizeTooSmall, ProduceResponse, FetchResponse,
    OffsetAndMessage, BrokerMetadata, PartitionMetadata
)
from kafka.common import KafkaUnavailableError
from kafka.codec import (
    has_gzip, has_snappy, gzip_encode, gzip_decode,
    snappy_encode, snappy_decode
)
from kafka.protocol import (
    create_gzip_message, create_message, create_snappy_message, KafkaProtocol
)

ITERATIONS = 1000
STRLEN = 100


def random_string():
    return os.urandom(random.randint(1, STRLEN))


class TestPackage(unittest.TestCase):

    def test_top_level_namespace(self):
        import kafka as kafka1
        self.assertEquals(kafka1.KafkaClient.__name__, "KafkaClient")
        self.assertEquals(kafka1.client.__name__, "kafka.client")
        self.assertEquals(kafka1.codec.__name__, "kafka.codec")

    def test_submodule_namespace(self):
        import kafka.client as client1
        self.assertEquals(client1.__name__, "kafka.client")
        self.assertEquals(client1.KafkaClient.__name__, "KafkaClient")

        from kafka import client as client2
        self.assertEquals(client2.__name__, "kafka.client")
        self.assertEquals(client2.KafkaClient.__name__, "KafkaClient")

        from kafka.client import KafkaClient as KafkaClient1
        self.assertEquals(KafkaClient1.__name__, "KafkaClient")

        from kafka.codec import gzip_encode as gzip_encode1
        self.assertEquals(gzip_encode1.__name__, "gzip_encode")

        from kafka import KafkaClient as KafkaClient2
        self.assertEquals(KafkaClient2.__name__, "KafkaClient")

        from kafka.codec import snappy_encode
        self.assertEquals(snappy_encode.__name__, "snappy_encode")


class TestCodec(unittest.TestCase):

    @unittest.skipUnless(has_gzip(), "Gzip not available")
    def test_gzip(self):
        for i in range(ITERATIONS):
            s1 = random_string()
            s2 = gzip_decode(gzip_encode(s1))
            self.assertEquals(s1, s2)

    @unittest.skipUnless(has_snappy(), "Snappy not available")
    def test_snappy(self):
        for i in xrange(ITERATIONS):
            s1 = random_string()
            s2 = snappy_decode(snappy_encode(s1))
            self.assertEquals(s1, s2)

    @unittest.skipUnless(has_snappy(), "Snappy not available")
    def test_snappy_detect_xerial(self):
        import kafka as kafka1
        _detect_xerial_stream = kafka1.codec._detect_xerial_stream

        header = b'\x82SNAPPY\x00\x00\x00\x00\x01\x00\x00\x00\x01Some extra bytes'
        false_header = b'\x01SNAPPY\x00\x00\x00\x01\x00\x00\x00\x01'
        random_snappy = snappy_encode('SNAPPY' * 50)
        short_data = b'\x01\x02\x03\x04'

        self.assertTrue(_detect_xerial_stream(header))
        self.assertFalse(_detect_xerial_stream(b''))
        self.assertFalse(_detect_xerial_stream(b'\x00'))
        self.assertFalse(_detect_xerial_stream(false_header))
        self.assertFalse(_detect_xerial_stream(random_snappy))
        self.assertFalse(_detect_xerial_stream(short_data))

    @unittest.skipUnless(has_snappy(), "Snappy not available")
    def test_snappy_decode_xerial(self):
        header = b'\x82SNAPPY\x00\x00\x00\x00\x01\x00\x00\x00\x01'
        random_snappy = snappy_encode('SNAPPY' * 50)
        block_len = len(random_snappy)
        random_snappy2 = snappy_encode('XERIAL' * 50)
        block_len2 = len(random_snappy2)

        to_test = header \
            + struct.pack('!i', block_len) + random_snappy \
            + struct.pack('!i', block_len2) + random_snappy2 \

        self.assertEquals(snappy_decode(to_test), ('SNAPPY' * 50) + ('XERIAL' * 50))

    @unittest.skipUnless(has_snappy(), "Snappy not available")
    def test_snappy_encode_xerial(self):
        to_ensure = b'\x82SNAPPY\x00\x00\x00\x00\x01\x00\x00\x00\x01' + \
            '\x00\x00\x00\x18' + \
            '\xac\x02\x14SNAPPY\xfe\x06\x00\xfe\x06\x00\xfe\x06\x00\xfe\x06\x00\x96\x06\x00' + \
            '\x00\x00\x00\x18' + \
            '\xac\x02\x14XERIAL\xfe\x06\x00\xfe\x06\x00\xfe\x06\x00\xfe\x06\x00\x96\x06\x00'

        to_test = ('SNAPPY' * 50) + ('XERIAL' * 50)

        compressed = snappy_encode(to_test, xerial_compatible=True, xerial_blocksize=300)
        self.assertEquals(compressed, to_ensure)


class TestKafkaClient(unittest.TestCase):

    def test_init_with_list(self):

        with patch.object(KafkaClient, 'load_metadata_for_topics'):
            client = KafkaClient(
                hosts=['kafka01:9092', 'kafka02:9092', 'kafka03:9092'])

        self.assertListEqual(
            [('kafka01', 9092), ('kafka02', 9092), ('kafka03', 9092)],
            sorted(client.hosts))

    def test_init_with_csv(self):

        with patch.object(KafkaClient, 'load_metadata_for_topics'):
            client = KafkaClient(
                hosts='kafka01:9092,kafka02:9092,kafka03:9092')

        self.assertListEqual(
            [('kafka01', 9092), ('kafka02', 9092), ('kafka03', 9092)],
            sorted(client.hosts))

    def test_init_with_unicode_csv(self):

        with patch.object(KafkaClient, 'load_metadata_for_topics'):
            client = KafkaClient(
                hosts=u'kafka01:9092,kafka02:9092,kafka03:9092')

        self.assertListEqual(
            [('kafka01', 9092), ('kafka02', 9092), ('kafka03', 9092)],
            sorted(client.hosts))

    def test_send_broker_unaware_request_fail(self):
        'Tests that call fails when all hosts are unavailable'

        mocked_conns = {
            ('kafka01', 9092): MagicMock(),
            ('kafka02', 9092): MagicMock()
        }
        # inject KafkaConnection side effects
        mocked_conns[('kafka01', 9092)].send.side_effect = RuntimeError("kafka01 went away (unittest)")
        mocked_conns[('kafka02', 9092)].send.side_effect = RuntimeError("Kafka02 went away (unittest)")

        def mock_get_conn(host, port):
            return mocked_conns[(host, port)]

        # patch to avoid making requests before we want it
        with patch.object(KafkaClient, 'load_metadata_for_topics'), \
                patch.object(KafkaClient, '_get_conn', side_effect=mock_get_conn):

            client = KafkaClient(hosts=['kafka01:9092', 'kafka02:9092'])

            self.assertRaises(
                KafkaUnavailableError,
                client._send_broker_unaware_request,
                1, 'fake request')

            for key, conn in mocked_conns.items():
                conn.send.assert_called_with(1, 'fake request')

    def test_send_broker_unaware_request(self):
        'Tests that call works when at least one of the host is available'

        mocked_conns = {
            ('kafka01', 9092): MagicMock(),
            ('kafka02', 9092): MagicMock(),
            ('kafka03', 9092): MagicMock()
        }
        # inject KafkaConnection side effects
        mocked_conns[('kafka01', 9092)].send.side_effect = RuntimeError("kafka01 went away (unittest)")
        mocked_conns[('kafka02', 9092)].recv.return_value = 'valid response'
        mocked_conns[('kafka03', 9092)].send.side_effect = RuntimeError("kafka03 went away (unittest)")

        def mock_get_conn(host, port):
            return mocked_conns[(host, port)]

        # patch to avoid making requests before we want it
        with patch.object(KafkaClient, 'load_metadata_for_topics'), \
                patch.object(KafkaClient, '_get_conn', side_effect=mock_get_conn):

            client = KafkaClient(hosts='kafka01:9092,kafka02:9092')

            resp = client._send_broker_unaware_request(1, 'fake request')

            self.assertEqual('valid response', resp)
            mocked_conns[('kafka02', 9092)].recv.assert_called_with(1)


if __name__ == '__main__':
    unittest.main()
