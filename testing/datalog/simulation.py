#!/usr/bin/python3
import paho.mqtt.client as paho
import argparse
import json
import time
import random
from datetime import datetime
import sys

# broker="127.0.0.1"
port=1883
# topic= 'message/sensor/l0v2p5'

broker="103.106.72.181"
topic="sensor/logger2"

def on_publish(client,userdata,result): #create function for callback
    print("data published")
    pass

client1= paho.Client("iot") #create client object
# client1.on_publish = on_publish  #assign function to callback
client1.connect(broker,port) #establish connection

cekpoint = 100
cekpoint_state = False

value = 25.5

msg = {
    "experiment_code":"B1"
}
payload = json.dumps(msg)
print(payload)
client1.publish("logger/start2",payload=payload) #publish

for x in range(1000):
    add = random.randint(1,5000) / 100
    if(not cekpoint_state):
        value += add
        if(value>cekpoint):
            cekpoint_state = True
    
    if(cekpoint_state):
        value = cekpoint + random.randint(1,500) / 100

    msg = {
        "ch_1":value
    }
    payload = json.dumps(msg)
    print(payload)
    client1.publish(topic,payload=payload) #publish
    time.sleep(2)

client1.close()
