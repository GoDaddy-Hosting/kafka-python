import unittest
import time

from unittest.mock import MagicMock, patch
from kafka.async_producer import AsyncProducer, thread_entry
from kafka.common import ProduceRequest, Message

class TestAsyncProducer(unittest.TestCase):

    def setUp(self):
        self.client = MagicMock()
        self.producer = AsyncProducer(self.client)

    def test_construct_producer(self):
        self.assertIsInstance(self.producer, AsyncProducer)

    def test_batch_seconds_setter(self):
        self.producer.batch_seconds = 3600
        self.assertEqual(self.producer.batch_seconds, 3600)

    def test_batch_seconds_exception(self):
        with self.assertRaises(ValueError):
            self.producer.batch_seconds = -1

    def test_batch_size(self):
        self.producer.batch_size = 10
        self.assertEqual(self.producer.batch_size, 10)

    def test_batch_size_exception(self):
        with self.assertRaises(ValueError):
            self.producer.batch_size = -1

    @patch('multiprocessing.Process.start')
    def test_starting_async_thread_start(self, process_start):
        self.producer.start()
        self.assertTrue(process_start.called)


    def test_start_inner_loop_client_reinit(self):
        self.producer._queue.put((AsyncProducer.STOP_ASYNC_PRODUCER, None))

        self.producer._start_message_loop()
        self.assertTrue(self.producer._client.reinit.called)

    def test_send_message_with_buffer_size_1(self):
        self.producer.send_messages("provisioning", 1, "test")
        self.producer._queue.put((AsyncProducer.STOP_ASYNC_PRODUCER, None))

        messages = []
        messages.append(Message(magic=0, attributes=0, key=None, value='test'))
        request = ProduceRequest('provisioning', 1, messages)

        self.producer._start_message_loop()
        self.producer._client.send_produce_request.assert_called_with([request], timeout=1000, acks=1)

    def test_send_message_with_buffer_size_2(self):
        self.producer.send_messages("provisioning", 1, "test1")
        self.producer.send_messages("provisioning", 1, "test2")
        self.producer._queue.put((AsyncProducer.STOP_ASYNC_PRODUCER, None))

        self.producer._start_message_loop()
        self.assertEqual(self.producer._client.send_produce_request.call_count, 2)

    def test_send_message_with_timeout(self):
        self.producer.batch_seconds = 3
        self.producer.batch_size = 10

        self.producer.send_messages("provisioning", 1, "test1")
        self.producer.send_messages("provisioning", 1, "test2")
        self.producer.send_messages("provisioning", 1, "test3")
        self.producer._queue.put((AsyncProducer.STOP_ASYNC_PRODUCER, None))

        messages = [
                Message(0, 0, None, "test1"),
                Message(0, 0, None, "test2"),
                Message(0, 0, None, "test3")
            ]

        expected = ProduceRequest("provisioning", 1, messages)
        self.producer._start_message_loop()
        self.producer._client.send_produce_request.assert_called_with([expected], acks=1, timeout=1000)

if __name__ == '__main__':
    unittest.main()
