#!/usr/bin/python3
import paho.mqtt.client as paho
import json
from datetime import datetime
import random

def writeLog(file,value):
    with open("logg/log_"+file+".txt",'a+',newline='') as f:
        f.write(value+'\n')

broker= '103.106.72.188' #"localhost" #"161.117.58.227"#
port=1883
topic= '/simulationIPS/client2'#'/Project_IPS/client1'#'message/sensor/xzhu2l''trydevice'#
topic2= 'trydevice2'#'Project_IPS/client1'#'message/sensor/xzhu2l'
device_code = "hl36"


def on_publish(client,userdata,result): #create function for callback
    print("data published")
    pass

client1= paho.Client("iot") #create client object
client1.on_publish = on_publish  #assign function to callback
# client1.username_pw_set(username="OGRhNTI5MzE1YjY0ZWRlN2EwNjI2Mzg1",password="hdMFWDGTnfbhfoxoW7YXU8IwyAhFbD") #userpass
client1.connect(broker,port) #establish connection
today = datetime.today() #current-datetime
# msg = {
#     # "device_code":"xzhu2l-so62",
#     # "date_add": round(datetime.today().timestamp() * 1000)-700, #today.strftime("%Y-%m-%d %H:%M:%S"),
#     # "gps":{
#     #     "latitude":-7.575973,
#     #     "longitude":112.878304
#     # },
#     # "temperature": 25.5,
#     # "fuel":900
#     "ts": round(datetime.today().timestamp() * 1000)-700, #today.strftime("%Y-%m-%d %H:%M:%S"),
# 	"id":"11",
# 	"lq":random.randint(0,100),
# 	"x":random.randint(4000,10000) / 100,
# 	"y":random.randint(4000,10000) / 100,
# 	"z":random.randint(4000,10000) / 100
# }
# payload = json.dumps(msg)
payload = "::rc=80000000:lq=59\u0000:ct=A8A6:ed=810D731C:id=3:ba=2540:a1=1314:a2=0665:x=1:y=1:z=1::ts=9774"
# payload = "::rc=8000\u00000000:lq=\u0000"
cle

now = datetime.now()
cDate = now.strftime("%Y-%m-%d")
cTime = now.strftime("%H:%M:%S")
cUnix = int(now.timestamp() * 1000)
ret= client1.publish(topic,payload=payload) #publish
writeLog(broker+"_"+device_code+"_"+cDate,cDate+","+cTime+","+str(cUnix)+","+payload)
# ret= client1.publish(topic2,payload=payload) #publish