import sys, json, time
import paho.mqtt.client as mqttClient #Must Install Req
import datetime
mqttConnect = False
broker_address= "localhost"
port = 1883                         
# user = "yourUser"
# password = "yourPassword"
client = mqttClient.Client("Python")  

def default(o):
    if isinstance(o, (datetime.date, datetime.datetime)):
        return o.isoformat()

def publish(topic,message):
	try:
		client.connect(broker_address, port=port)
		time.sleep(0.5)
		client.publish(topic,json.dumps(message,default=default))
		client.disconnect()
	except  Exception as e:
		print("failed")
		print(e)

	return
