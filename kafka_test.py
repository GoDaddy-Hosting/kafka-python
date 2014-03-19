from kafka.client import KafkaClient
from kafka.producer import SimpleProducer, KeyedProducer

kafka = KafkaClient("192.168.101.11:9092")

producer = SimpleProducer(kafka, async=False)
response = producer.send_messages(b"provisioning", b"test message")

print(response)

