from __future__ import absolute_import

import logging
import time

from kafka.common import ProduceRequest, TopicAndPartition
from kafka.partitioner import HashedPartitioner
from kafka.protocol import create_message

log = logging.getLogger("kafka")

class Producer(object):

    ACK_NOT_REQUIRED = 0            # No ack is required
    ACK_AFTER_LOCAL_WRITE = 1       # Send response after it is written to log
    ACK_AFTER_CLUSTER_COMMIT = -1   # Send response after data is committed

    DEFAULT_ACK_TIMEOUT = 1000

    @property
    def require_ack(self):
        return self._req_ack

    @require_ack.setter
    def require_ack(self, value):
        if ( value not in (self.ACK_NOT_REQUIRED, self.ACK_AFTER_LOCAL_WRITE, self.ACK_AFTER_CLUSTER_COMMIT) ):
            raise ValueError('require_ack must be set to one of the following: ACK_NOT_REQUIRED, ACK_AFTER_LOCAL_WRITE, ACK_AFTER_CLUSTER_COMMIT')

        self._req_ack = value

    @property
    def ack_timeout(self):
        return self._ack_timeout

    @ack_timeout.setter
    def ack_timeout(self, value):
        if (value < 0):
            raise ValueError('ack_timeout must be greater or equal to zero')

        self._ack_timeout = value

    def __init__(self, client):
        self._req_ack = Producer.ACK_AFTER_LOCAL_WRITE
        self._ack_timeout = Producer.DEFAULT_ACK_TIMEOUT
        self._client = client

    def send_messages(self, topic, partition, *msg):
        """
        Helper method to send produce requests
        """

        messages = [create_message(m) for m in msg]
        req = ProduceRequest(topic, partition, messages)
        try:
            resp = self._client.send_produce_request([req], acks=self._req_ack, timeout=self._ack_timeout)
        except Exception:
            log.exception("Unable to send messages")
            raise

        return resp

