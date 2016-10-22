from kafka import KafkaClient, SimpleProducer
import json,requests

kafka = KafkaClient('localhost:9092')

producer = SimpleProducer(kafka)

r = requests.get("https://stream.meetup.com/2/rsvps",stream=True)

for line in r.iter_lines():
	producer.send_messages('meetup',line)
	print type(line)

kafka.close()