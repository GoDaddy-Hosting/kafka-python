from __future__ import absolute_import

import logging
import time

from queue import Empty

from collections import defaultdict
from itertools import cycle
from multiprocessing import Queue, Process

from kafka.common import ProduceRequest, TopicAndPartition
from kafka.partitioner import HashedPartitioner
from kafka.protocol import create_message
from kafka.producer import Producer

log = logging.getLogger("kafka")

def thread_entry(producer):
    producer._start_message_loop()

class AsyncProducer(Producer):
    BATCH_SEND_DEFAULT_INTERVAL = 120
    BATCH_SEND_MSG_COUNT = 1
    STOP_ASYNC_PRODUCER = -1

    @property
    def batch_seconds(self):
        return self._batch_seconds

    @batch_seconds.setter
    def batch_seconds(self, value):
        if value < 1:
            raise ValueError('batch_seconds must be greater than 0 seconds')

        self._batch_seconds = value

    @property
    def batch_size(self):
        return self._batch_size

    @batch_size.setter
    def batch_size(self, value):
        if value < 1:
            raise ValueError('batch_size must be greater than 0 messages')

        self._batch_size = value

    def __init__(self, client):
        super(AsyncProducer, self).__init__(client)

        self._batch_seconds = self.BATCH_SEND_DEFAULT_INTERVAL
        self._batch_size = self.BATCH_SEND_MSG_COUNT
        self._queue = Queue()

    def send_messages(self, topic, partition, *msg):
        for m in msg:
            self._queue.put((TopicAndPartition(topic, partition), create_message(m)))

        return []

    def start(self):
        self.proc = Process(target=thread_entry, args=(self,))

        # Process will die if main thread exits
        self.proc.daemon = True
        self.proc.start()

    def _read_message_queue(self):
        messages = defaultdict(list)

        count = self._batch_size
        timeout = self._batch_seconds
        send_at = time.time() + self._batch_seconds

        # Keep fetching till we gather enough messages or a timeout is reached
        while count > 0 and timeout >= 0:
            try:
                topic_partition, message = self._queue.get(timeout=timeout)

            except Empty:
                break

            # Check if the controller has requested us to stop
            if topic_partition == AsyncProducer.STOP_ASYNC_PRODUCER:
                self._stop_message_loop = True
                break

            messages[topic_partition].append(message)

            # Adjust the timeout to match the remaining period
            count -= 1
            timeout = send_at - time.time()

        return messages

    def _send_message_queue(self, message_queue):
        requests = []

        for topic_partition, messages in message_queue.items():
            request = ProduceRequest(topic_partition.topic, topic_partition.partition, messages)
            requests.append(request)

        try:
            self._client.send_produce_request(requests, acks=self.require_ack, timeout=self.ack_timeout)

        except Exception as e:
            log.exception("Unable to send message")

    def _start_message_loop(self):
        self._stop_message_loop = False
        self._client.reinit()

        while not self._stop_message_loop:
            messages = self._read_message_queue()
            if ( len(messages) > 0 ):
                self._send_message_queue(messages)

    def stop(self, timeout=1):
        """
        Stop the producer. Optionally wait for the specified timeout before
        forcefully cleaning up.
        """
        self._queue.put((AsyncProducer.STOP_ASYNC_PRODUCER, None))
        self.proc.join(timeout)

        if self.proc.is_alive():
            self.proc.terminate()

