#!/usr/bin/python3
import paho.mqtt.client as paho
import argparse
import json
import time
import random
from datetime import datetime
import sys



def on_publish(client,userdata,result): #create function for callback
    print("data published")
    pass







port=1883 #MQTT PORT
broker="103.106.72.188" #MQTT Broker URL (SEMAR Server)
client1= paho.Client("sensor_node1") #create client object
client1.connect(broker,port) #establish connection
topic="environment/node2" #MQTT topic registered on the server
sensor = {
    "temperature":29,
    "humidity":75.8
}
client1.publish(topic,payload=json.dumps(sensor)) #publish

# port=1883 #MQTT PORT
# broker="103.106.72.188" #MQTT Broker URL (SEMAR Server)
# client1= paho.Client("sensor_node1") #create client object
# client1.connect(broker,port) #establish connection

# for i in range(1000):
#     for l in range(1,4):
#         topic="environment/node"+str(l) #MQTT topic registered on the server
#         sensor = {
#             "temperature":random.randint(28,35),
#             "humidity":random.randint(7500,8000)/100,
#             "id":1
#         }
#         client1.publish(topic,payload=json.dumps(sensor)) #publish        

#     time.sleep(5)
#     print("send")