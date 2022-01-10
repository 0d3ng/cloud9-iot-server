from os import SEEK_END
import sys
import paho.mqtt.client as paho
import json
import time
import random
from datetime import datetime
broker="localhost" #103.106.72.188
port=1883
list_topic = ['Project_IPS/client1','Project_IPS/client2','Project_IPS/client3','Project_IPS/client4','Project_IPS/client5']

try:
    client= paho.Client("AC-GuideDevicesD206")
    client.connect(broker,port)
except:
    print("Failed")
    sys.stdout.flush()

def sendMQTT(topic,msg):
    payload = json.dumps(msg)
    client.publish(topic,payload=payload)
    print("Topic ",topic)
    sys.stdout.flush()

def stopMQTT():
    client.disconnect()

for x in range(10):
    for y in range(5):
        for i in range(5):
            msg = {
                "id":str(y+1),
                "lq":random.randint(5,100)
            }
            sendMQTT(topic=list_topic[i],msg=msg)
    time.sleep(10)

stopMQTT()