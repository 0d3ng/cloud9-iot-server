import multiprocessing
import threading
import sys, json, time
import paho.mqtt.client as mqttClient #Must Install Req
from function import *
from controller import filterController,deviceController
from datetime import datetime,timedelta
from configparser import ConfigParser
from importlib import reload
from pytz import timezone

config = ConfigParser()
config.read("config.ini")
#Config
Connected = False
broker_address= config["MQTT"]["broker"]
port = int(config["MQTT"]["port"])                         
user = config["MQTT"]["user"]
password = config["MQTT"]["pass"]

def on_connect(client, userdata, flags, rc):
    print("rc: "+str(rc))
    sys.stdout.flush()
    if rc == 0:
        print("Connected to broker")
        print("------------------------------------")
        client.subscribe(config["MQTT"]["datasync_stream_start"])
        client.subscribe(config["MQTT"]["datasync_stream_stop"])
        print(config["MQTT"]["datasync_stream_start"]," - ",config["MQTT"]["datasync_stream_stop"])
        # stream_list()             
    else:
        print("Connection failed")
        sys.stdout.flush()


def on_message(client, userdata, message):
    raw_msg = message.payload.decode("utf-8")
    try:
        raw_object = json.loads(raw_msg)
        if message.topic == config["MQTT"]["filter_start"] :
            on_service(raw_object)
        elif message.topic == config["MQTT"]["filter_remove"] :
            on_remove(raw_object)
        else:
            on_process(message.topic,raw_object)
    except:
        raw_object = {"failed":True}

def on_service(message):
    #Get...
    #Update...
    print(message)

def on_remove(message):
    print(message)

def on_process(topic,message):
    print(message)


def get_config(file):
    # {
    #     "config":{
    #         "method":"-"
    #         "parameters":{
    #             "---":
    #             "---":
    #         },
    #         "waiting_time":...,
    #         "group":[...,...]
    #     },
    #     "data":{
    #         "prev_time":.....[unix]
    #         "prev_data":[]
    #         "prev_filter":[]
    #     }
    # }
    print(file)

def update_config(file):
    print(file)

def remove_config(file):
    print(file)





# "mqtt/output/device-[device_code]"

if __name__ == "__main__":
    client = mqttClient.Client("Python3"+cloud9Lib.randomOnlyString(4))               
    # client.username_pw_set(username=user, password=password)    #set username and password
    client.on_connect= on_connect                      
    client.on_message= on_message                      
    client.connect(broker_address, port=port)          
    client.loop_start()  
    try:
        while True:
            time.sleep(1)
    
    except KeyboardInterrupt:
        print("exiting")
        client.disconnect()
        client.loop_stop()