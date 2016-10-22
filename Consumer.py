from kafka import KafkaConsumer

consumer = KafkaConsumer('meetup', group_id = '1', bootstrap_servers = ['localhost:9092'])

for message in consumer:
	print message.value