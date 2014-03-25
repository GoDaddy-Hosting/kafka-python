from __future__ import absolute_import

from kafka.producer import Producer
from kafka.partitioner import HashedPartitioner

class KeyedProducer(Producer):
    """
    A producer which distributes messages to partitions based on the key

    Args:
    producer - The kafka client instance
    partitioner - A partitioner class that will be used to get the partition
        to send the message to. Must be derived from Partitioner
    """
    def __init__(self, producer, partitioner=None):

        self.producer = producer

        if not partitioner:
            partitioner = HashedPartitioner

        self.partitioner_class = partitioner
        self.partitioners = {}

    def _next_partition(self, topic, key):
        if topic not in self.partitioners:
            if topic not in self.client.topic_partitions:
                self.client.load_metadata_for_topics(topic)
            self.partitioners[topic] = self.partitioner_class(self.client.topic_partitions[topic])
        partitioner = self.partitioners[topic]
        return partitioner.partition(key, self.client.topic_partitions[topic])

    def send_message(self, topic, key, msg):
        partition = self._next_partition(topic, key)
        return self.producer.send_messages(topic, partition, msg)

    def __repr__(self):
        return '<KeyedProducer batch=%s>' % self.async
