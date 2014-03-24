import unittest
from unittest.mock import MagicMock, call
from kafka.consumer import Consumer, SimpleConsumer
from kafka.common import OffsetRequest, OffsetCommitRequest

class TestConsumer(unittest.TestCase):
    def setUp(self):
        self.mock_client = MagicMock()
        self.consumer = Consumer(self.mock_client, "test_group", "test_topic")

    def test_consumer_init(self):
        self.assertEqual(self.consumer.group, "test_group")

    def test_commit(self):
        self.consumer.count_since_commit = 1
        self.consumer.offsets = {0:1}

        expected_reqs = [OffsetCommitRequest(topic='test_topic', partition=0, offset=1, metadata=None)]
        expected_call = call('test_group', expected_reqs)
        self.consumer.commit()

        self.mock_client.send_offset_commit_request.assert_called_with('test_group', expected_reqs)

    def test_stop(self):
        self.consumer.commit_timer = MagicMock()
        self.consumer.commit = MagicMock()

        self.consumer.stop()
        self.assertTrue(self.consumer.commit.called)

    def test_pending(self):
        self.consumer.offsets = {0:1}

        resps = [MagicMock(partition=0,offsets=[3])]
        self.mock_client.send_offset_request = MagicMock(return_value=resps)
        total = self.consumer.pending()
        self.assertEqual(total, 1)

class TestSimpleConsumer(unittest.TestCase):
    def setUp(self):
        self.mock_client = MagicMock()
        self.consumer = SimpleConsumer(self.mock_client, "test_group", "test_topic")

    def test_provide_partition_info(self):
        self.assertTrue(self.consumer.provide_partition_info)

    def test_seek_whence_0(self):
        self.consumer.seek_head = MagicMock()
        self.consumer.seek(1, 0)

        self.assertTrue(self.consumer.seek_head.called)

    def test_seek_whence_1(self):
        self.consumer.seek_current = MagicMock()
        self.consumer.seek(1, 1)

        self.assertTrue(self.consumer.seek_current.called)

    def test_seek_whence_2(self):
        self.consumer.seek_tail = MagicMock()
        self.consumer.seek(1, 2)

        self.assertTrue(self.consumer.seek_tail.called)

    def test_seek_whence_unknown(self):
        with self.assertRaises(ValueError):
            self.consumer.seek(1, 4)

    def test_seek_current(self):
        self.consumer._offset = 1
        self.consumer.offsets = {0:1}
        self.consumer.seek_current(1)

        self.assertEqual(self.consumer.offsets[0], 2)

    def test_seek_head(self):
        self.consumer.send_offset_requests = MagicMock()
        self.consumer.offsets = {0:1, 1:1}
        self.consumer.seek_head(1)
        requests = [
            OffsetRequest(topic='test_topic', partition=0, time=-2, max_offsets=1),
            OffsetRequest(topic='test_topic', partition=1, time=-2, max_offsets=1)
        ]
        self.consumer.send_offset_requests.assert_called_with(requests)

    def test_seek_tail(self):
        self.consumer.send_offset_requests = MagicMock()
        self.consumer.offsets = {1:1}
        self.consumer.seek_tail(1)
        requests = [
            OffsetRequest(topic='test_topic', partition=1, time=-1, max_offsets=1)
        ]
        self.consumer.send_offset_requests.assert_called_with(requests)

    def test_get_message(self):
        message = MagicMock(offset=0)
        self.consumer.queue = MagicMock(get_nowait=MagicMock(return_value=(0, message)))
        msg = self.consumer.get_message()
        self.assertEqual(msg, message)

