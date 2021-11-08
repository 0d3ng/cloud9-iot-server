import sys
from time import sleep
from json import dumps
from kafka import KafkaProducer

broker_address = "103.56.148.215"
port = "9092"

def publish(topic,message):
	try:
		print(topic)
		print(message)
		sys.stdout.flush()
		producer = KafkaProducer(bootstrap_servers=[broker_address+':'+port],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))
		producer.send(topic, value=message)
	except:
		print("failed")
	return