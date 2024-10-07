import sys, json, time
import paho.mqtt.client as mqttClient #Must Install Req
import datetime
from configparser import ConfigParser
import random,string

from logger import setup_logger
logger = setup_logger(to_file=False)

config = ConfigParser()
config.read("config.ini")
#Config
mqttConnect = False
broker_address= config["MQTT"]["broker"]
port = int(config["MQTT"]["port"])                         
# user = config["MQTT"]["user"]
# password = config["MQTT"]["pass"]  

client = mqttClient.Client()
client.connect(broker_address, port=port)
logger.info(client.is_connected())

def randomString(stringLength=8):
    letters1 = string.ascii_lowercase
    letters2 = string.ascii_uppercase 
    letters3 = string.digits
    return ''.join(random.choice(letters1 + letters2 + letters3) for i in range(stringLength))

def default(o):
    if isinstance(o, (datetime.date, datetime.datetime)):
        return o.isoformat()

def publish(topic,message, print_out=True):
	try:
		def on_publish(client,userdata,result):             #create function for callback
			logger.info("data published \n")
			pass
		if topic == config["MQTT"]["datasync_stream_start"] or topic == config["MQTT"]["datasync_stream_stop"] or topic == config["MQTT"]["other_subscribe"] or topic == config["MQTT"]["other_unsubscribe"]:
			client.on_publish= on_publish
		client.publish(topic,json.dumps(message,default=default))
		if print_out:
			logger.info("------MQTT------")
			logger.info(topic)
			logger.info(message)
			logger.info("----------------")
		client.disconnect()
	except  Exception as e:
		logger.info("failed")
		print(e)

	return
