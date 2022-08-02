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

broker="localhost"#"103.106.72.188"#
port=1883
TM_wait = 1 #second
file_list = {}
topic = "/simulationIPS/client2"
device_code = "hl36"
max_rep = 500


def randomString(stringLength=8):
    letters1 = string.ascii_lowercase
    letters2 = string.ascii_uppercase 
    letters3 = string.digits
    return ''.join(random.choice(letters1 + letters2 + letters3) for i in range(stringLength))

def writeLog(file,value):
    with open("send_"+file+".txt",'a+',newline='') as f:
        f.write(value+'\n')

def start_simulation(id,topic,client1,file_goal):
    i = 0
    for i in range(max_rep):
        msg = {
            "t": round(datetime.today().timestamp() * 1000)-700, #today.strftime("%Y-%m-%d %H:%M:%S"),
            "id":id,
            "lq":random.randint(0,100),
            "x":random.randint(4000,10000) / 100,
            "y":random.randint(4000,10000) / 100,
            "z":random.randint(4000,10000) / 100
        }
        payload = json.dumps(msg)
        client1.publish(topic,payload=payload)
        now = datetime.now()
        cDate = now.strftime("%Y-%m-%d")
        cTime = now.strftime("%H:%M:%S")
        cUnix = int(now.timestamp() * 1000)
        raw_msg = payload
        writeLog(file_goal,cDate+","+cTime+","+str(cUnix)+","+raw_msg)
        time.sleep(TM_wait)

def on_publish(client,userdata,result): #create function for callback
    print("data published")
    pass

def worker(id,code,file_goal):   
    client1= mqttClient.Client("simulation_"+code) #create client object
    client1.connect(broker,port) #establish connection       
    print("Start Simulation : "+str(id))
    print(datetime.now().timestamp() * 1000)
    topic = "/simulationIPS/client-test-"+str(id)
    start_simulation(id,topic,client1,file_goal)     

def readfile(device_num,file_goal):
    code = randomString(3)
    for id in range(device_num):
        id = id+1
        p = multiprocessing.Process(target=worker, args=(id,code,file_goal))
        p.start()

if __name__ == '__main__':
    code = randomString(3)
    device_num = 5
    file_goal = str(device_num)+"_"+str(datetime.now().strftime("%Y-%m-%d_%H-%M-%S"))
    readfile(device_num,file_goal)
    # for i in range(max_rep):
    #     for i in range(device_num):
    #         msg = {
    #             "t": round(datetime.today().timestamp() * 1000)-700, #today.strftime("%Y-%m-%d %H:%M:%S"),
    #             "id":id,
    #             "lq":random.randint(0,100),
    #             "x":random.randint(4000,10000) / 100,
    #             "y":random.randint(4000,10000) / 100,
    #             "z":random.randint(4000,10000) / 100
    #         }
    #         payload = json.dumps(msg)
    #         client1.publish(topic,payload=payload)
    #         now = datetime.now()
    #         cDate = now.strftime("%Y-%m-%d")
    #         cTime = now.strftime("%H:%M:%S")
    #         cUnix = int(now.timestamp() * 1000)
    #         raw_msg = payload
    #         writeLog(file_goal,cDate+","+cTime+","+str(cUnix)+","+raw_msg)
    #         time.sleep(TM_wait)