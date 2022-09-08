#!/usr/bin/python3
import paho.mqtt.client as paho
import argparse
import json
import time
import random
from datetime import datetime
import sys



port=1883 #MQTT PORT
broker="103.106.72.188" #MQTT Broker URL (SEMAR Server)
client1= paho.Client("sensor_node1") #create client object
client1.connect(broker,port) #establish connection

for i in range(1000):
    topic="sensor/node1"#MQTT topic registered on the server
    sensor = {
        "temperature":29,      
        "ip":"192.168.0.1",
        "id":1
    }
    client1.publish(topic,payload=json.dumps(sensor)) #publish        
    time.sleep(5)
    print("send")