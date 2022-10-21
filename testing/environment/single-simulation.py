#!/usr/bin/python3
import paho.mqtt.client as paho
import argparse
import json
import time
import random
from datetime import datetime
import sys



port=1883 #MQTT PORT
broker="103.106.72.181" #MQTT Broker URL (SEMAR Server)
client1= paho.Client("sensor_node2") #create client object
client1.connect(broker,port) #establish connection

for i in range(1000):
    topic= "fils154/person"#"sensor\/node3"#MQTT topic registered on the server
    sensor = {
        # "temperature":random.randint(28,35), #32,          
        # "humidity":random.randint(75,80), #78, 
        # "no2":random.randint(75,80), #78,
        # "gps":{
        #     "lat":random.randint(107,144), #78,
        #     "lng":random.randint(-10,11), #78,
        # },
        # "id":1
        "person":1
    }
    client1.publish(topic,payload=json.dumps(sensor)) #publish        
    time.sleep(5)
    print("send")

# topic="sensor/node4"#MQTT topic registered on the server
# sensor = {
#     "temperature":random.randint(28,35), #32,          
#     "humidity":random.randint(75,80), #78, 
#     "id":1
# }
# client1.publish(topic,payload=json.dumps(sensor)) #publish        
# print("send")