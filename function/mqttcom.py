import sys, json, time
import paho.mqtt.client as mqttClient #Must Install Req
import datetime
from configparser import ConfigParser
config = ConfigParser()
config.read("config.ini")
#Config
mqttConnect = False
broker_address= config["MQTT"]["broker"]
port = int(config["MQTT"]["port"])                         
# user = config["MQTT"]["user"]
# password = config["MQTT"]["pass"]
client = mqttClient.Client("CloudIoTMQTT")  

def default(o):
    if isinstance(o, (datetime.date, datetime.datetime)):
        return o.isoformat()

def publish(topic,message):
	try:
		client.connect(broker_address, port=port)
		time.sleep(0.5)
		client.publish(topic,json.dumps(message,default=default))
		print("------MQTT------")
		print(topic)
		print(message)
		print("----------------")
		sys.stdout.flush()
		client.disconnect()
	except  Exception as e:
		print("failed")
		print(e)

	return
