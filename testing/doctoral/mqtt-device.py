import sys
import paho.mqtt.client as paho
import json
import time
import random
from datetime import datetime
broker="103.106.72.188"
port=1883
topic= 'message/sensor/jd2090'

try:
    client= paho.Client("AC-GuideDevicesD206")
    client.connect(broker,port)
except:
    print("Failed")
    sys.stdout.flush()

def sendMQTT(msg):
    payload = json.dumps(msg)
    client.publish(topic,payload=payload)

def stopMQTT():
    client.disconnect()

for x in range(3):
    today = datetime.today() #current-datetime
    msg = {
        "device_code":"jd2090-sv94",
        "date_add":round(datetime.today().timestamp() * 1000)-700, #today.strftime("%Y-%m-%d %H:%M:%S"),
        "indoor_temperature":random.randint(1500,3500) / 100,
        "indoor_humidity":random.randint(4000,10000) / 100,
        "indoor_di":0,
        "ac_state":1,
        "outdoor_temperature":15,
        "outdoor_humidity":83.4,
        "outdoor_di":0,
    }
    sendMQTT(msg)
    time.sleep(1)
stopMQTT()