from __future__ import absolute_import
from itertools import cycle

from kafka.producer import Producer

class SimpleProducer(object):
    """
    A simple, round-robbin producer. Each message goes to exactly one partition

    Params:
    producer - The producer object to use to send messages
    """

    def __init__(self, producer):
        self.producer = producer
        self.partition_cycles = {}

    def _next_partition(self, topic):
        if topic not in self.partition_cycles:
            if topic not in self.producer._client.topic_partitions:
                self.producer._client.load_metadata_for_topics(topic)
            self.partition_cycles[topic] = cycle(self.producer._client.topic_partitions[topic])

        return next(self.partition_cycles[topic])

    def send_messages(self, topic, *msg):
        partition = self._next_partition(topic)
        return self.producer.send_messages(topic, partition, *msg)

    def __repr__(self):
        return '<SimpleProducer batch=%s>' % self.async

