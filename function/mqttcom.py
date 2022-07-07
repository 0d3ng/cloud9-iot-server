import sys, json, time
import paho.mqtt.client as mqttClient #Must Install Req
import datetime
from configparser import ConfigParser
import random,string
config = ConfigParser()
config.read("config.ini")
#Config
mqttConnect = False
broker_address= config["MQTT"]["broker"]
port = int(config["MQTT"]["port"])                         
# user = config["MQTT"]["user"]
# password = config["MQTT"]["pass"]  

def randomString(stringLength=8):
    letters1 = string.ascii_lowercase
    letters2 = string.ascii_uppercase 
    letters3 = string.digits
    return ''.join(random.choice(letters1 + letters2 + letters3) for i in range(stringLength))

def default(o):
    if isinstance(o, (datetime.date, datetime.datetime)):
        return o.isoformat()

def publish(topic,message):
	client = mqttClient.Client("CloudIoTMQTT"+randomString(4))
	try:
		def on_publish(client,userdata,result):             #create function for callback
			print("data published \n")
			pass
		if(topic == config["MQTT"]["datasync_stream_start"] or topic == config["MQTT"]["datasync_stream_stop"] or topic == config["MQTT"]["other_subscribe"] or topic == config["MQTT"]["other_unsubscribe"]):
			client.on_publish= on_publish
		client.connect(broker_address, port=port)
		time.sleep(2)
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
