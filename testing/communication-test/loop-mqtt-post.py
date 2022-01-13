#!/usr/bin/python3
import paho.mqtt.client as paho
import argparse
import json
import time
import random
from datetime import datetime
import sys

broker="localhost"
port=1883
topic= 'message/sensor/jd2090'

def on_publish(client,userdata,result): #create function for callback
    print("data published")
    pass

client1= paho.Client("iot") #create client object
client1.on_publish = on_publish  #assign function to callback
# client1.username_pw_set(username="OGRhNTI5MzE1YjY0ZWRlN2EwNjI2Mzg1",password="hdMFWDGTnfbhfoxoW7YXU8IwyAhFbD") #userpass
client1.connect(broker,port) #establish connection

for x in range(1000):
    today = datetime.today() #current-datetime
    msg = {
        "device_code":"jd2090-go52",
        "date_add_sensor":round(datetime.today().timestamp() * 1000)-700, #today.strftime("%Y-%m-%d %H:%M:%S"),
        "sensing_time":today.strftime("%H:%M:%S"),
        "co2_value":random.randint(1500,3500) / 100,
        "window_state":0,
        "door_state":1,
        "person_num":2,
        "ventilate_state":1,
        "temperature_in":random.randint(500,3500) / 100,
        "humidity_in":random.randint(500,3500) / 100,
        "di_in":random.randint(500,3500) / 100,
        "ac_state":1
    }
    payload = json.dumps(msg)
    print(msg)
    sys.stdout.flush()
    client1.publish(topic,payload=payload) #publish
    time.sleep(1)

client1.close()
