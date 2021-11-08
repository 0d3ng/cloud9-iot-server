#!/usr/bin/python3
import paho.mqtt.client as paho
import json
from datetime import datetime

broker="127.0.0.1" #"161.117.58.227"
port=1883
topic= 'message/sensor/xzhu2l'

def on_publish(client,userdata,result): #create function for callback
    print("data published")
    pass

client1= paho.Client("iot") #create client object
client1.on_publish = on_publish  #assign function to callback
# client1.username_pw_set(username="OGRhNTI5MzE1YjY0ZWRlN2EwNjI2Mzg1",password="hdMFWDGTnfbhfoxoW7YXU8IwyAhFbD") #userpass
client1.connect(broker,port) #establish connection
today = datetime.today() #current-datetime
msg = {
    "device_code":"xzhu2l-so62",
    "date_add": round(datetime.today().timestamp() * 1000)-700, #today.strftime("%Y-%m-%d %H:%M:%S"),
    "gps":{
        "latitude":-7.575973,
        "longitude":112.878304
    },
    "temperature": 25.5,
    "fuel":900
}
payload = json.dumps(msg)
ret= client1.publish(topic,payload=payload) #publish