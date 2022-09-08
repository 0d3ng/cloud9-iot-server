from http import client
from multiprocessing.connection import wait
import os, shutil
import sys, json, time
from datetime import datetime
import random,string
import multiprocessing
from scipy.fftpack import ss_diff
import glob
import pandas as pd
import paho.mqtt.client as mqttClient
import threading

broker="103.106.72.188"#"localhost"#
port=1883
TM_wait = 1 #second
file_list = {}
topic_l = []
device_num = 40
for i in range(device_num):
    topic_l.append("/simulationIPS/client-test-"+str(i+1))

max_rep = 500
file_goal = str(device_num)+"_"+str(datetime.now().strftime("%Y-%m-%d_%H-%M-%S"))
comm_pubs = {}


def randomString(stringLength=8):
    letters1 = string.ascii_lowercase
    letters2 = string.ascii_uppercase 
    letters3 = string.digits
    return ''.join(random.choice(letters1 + letters2 + letters3) for i in range(stringLength))

def on_publish(client,userdata,result): #create function for callback
    print("publish ")
    pass

def writeLog(folder,file,value):
    with open(str(folder)+"/send_"+str(file)+".txt",'a+',newline='') as f:
        f.write('\n'+value)

def worker(id,code,file_goal,topic):
    print(datetime.now().timestamp() * 1000)    
    print("Start Simulation : "+str(id))
    i = 0
    print(client1)
    for i in range(max_rep):
        msg = {
            "t": round(datetime.today().timestamp() * 1000)-700,
            "id":id,
            "lq":random.randint(0,100)
        }
        payload = json.dumps(msg)
        client1.publish(topic,payload=payload)
        now = datetime.now()
        cDate = now.strftime("%Y-%m-%d")
        cTime = now.strftime("%H:%M:%S")
        cUnix = int(now.timestamp() * 1000)
        raw_msg = payload
        writeLog(device_num,id,cDate+";"+cTime+";"+str(cUnix)+";"+raw_msg)
        time.sleep(TM_wait)

def on_connect(client, userdata, flags, rc):
    print("rc: "+str(rc))
    if rc == 0:
        i = 0
        for topic in topic_l:       
            i=i+1   
            # p = multiprocessing.Process(target=worker, args=(id,code,file_goal,topic,listid))
            p =  threading.Thread(target=worker, args=(i,code,file_goal,topic))
            p.start()    
    else:
        print("Connection failed")    

if __name__ == '__main__':
    code = randomString(3)    
    client1 = mqttClient.Client("digi_mqtt_test"+code)  # Create instance of client with client ID “digi_mqtt_test”
    client1.on_connect = on_connect  # Define callback function for successful connection
    # client1.on_publish = on_publish  # Define callback function for receipt of a message
    # client.connect("m2m.eclipse.org", 1883, 60)  # Connect to (broker, port, keepalive-time)
    client1.connect(broker, 1883)
    client1.loop_forever()  # Start networking daemon
    # print(client)
    # readfile(device_num,file_goal)    