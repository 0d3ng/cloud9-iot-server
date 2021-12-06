#!/usr/bin/python3
import paho.mqtt.client as paho
import argparse
import json
import time
import random
from datetime import datetime
import sys

broker="127.0.0.1"
port=1883
topic= 'message/sensor/l0v2p5'

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
        "device_code":"l0v2p5-co62",
        "date_add":round(datetime.today().timestamp() * 1000)-700, #today.strftime("%Y-%m-%d %H:%M:%S"),
        "layanan_kependudukan":random.randint(2,100),
        "layanan_catatan_cipil":random.randint(2,100),
    }
    payload = json.dumps(msg)
    print(msg)
    sys.stdout.flush()
    client1.publish(topic,payload=payload) #publish
    time.sleep(1)

client1.close()
