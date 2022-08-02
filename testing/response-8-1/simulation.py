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
# topic_l = ["/simulationIPS/client1","/simulationIPS/client2","/simulationIPS/client3","/simulationIPS/client4","/simulationIPS/client5","/simulationIPS/client6","/simulationIPS/client7",
#             "/simulationIPS/client8","/simulationIPS/client9","/simulationIPS/client10","/simulationIPS/client11","/simulationIPS/client12","/simulationIPS/client13","/simulationIPS/client14"
#             ,"/simulationIPS/client15","/simulationIPS/client16","/simulationIPS/client17","/simulationIPS/client18","/simulationIPS/client19","/simulationIPS/client20"]

topic_l = []
for i in range(35):
    topic_l.append("/simulationIPS/client"+str(i+1))

device_code = "hl36"
max_rep = 500
device_num = 500
file_goal = str(device_num)+"_"+str(datetime.now().strftime("%Y-%m-%d_%H-%M-%S"))
comm_pubs = {}


def randomString(stringLength=8):
    letters1 = string.ascii_lowercase
    letters2 = string.ascii_uppercase 
    letters3 = string.digits
    return ''.join(random.choice(letters1 + letters2 + letters3) for i in range(stringLength))

# class Comm:
#     def __init__(self, id, broker, port, topic, file_goal, listid):
#         self.id = id
#         self.broker = broker
#         self.port = int(port)
#         self.topic = topic
#         self.file_goal = file_goal
#         self.listid = listid
        
#     def on_connect(self, client, userdata, flags, rc):
#         print("rc: "+str(rc))
#         if rc == 0:
#             # print("Connected to broker:"+self.broker+":"+str(self.port))
#             print(datetime.now().timestamp() * 1000)    
#             print("Start Simulation : "+str(self.id))
#             i = 0
#             for i in range(max_rep):
#                 for idx in self.listid:
#                     msg = {
#                         "t": round(datetime.today().timestamp() * 1000)-700, #today.strftime("%Y-%m-%d %H:%M:%S"),
#                         "id":idx,
#                         "lq":random.randint(0,100),
#                         "x":random.randint(4000,10000) / 100,
#                         "y":random.randint(4000,10000) / 100,
#                         "z":random.randint(4000,10000) / 100
#                     }
#                     payload = json.dumps(msg)
#                     client.publish(self.topic,payload=payload)
#                     now = datetime.now()
#                     cDate = now.strftime("%Y-%m-%d")
#                     cTime = now.strftime("%H:%M:%S")
#                     cUnix = int(now.timestamp() * 1000)
#                     raw_msg = payload
#                     writeLog(self.file_goal,cDate+","+cTime+","+str(cUnix)+","+raw_msg)
                
            
#             time.sleep(TM_wait)
#         else:
#             print("Connection failed")

#     def on_publish(self,client,userdata,result): #create function for callback
#         print("publish "+str(self.id))
#         pass

#     def connect(self):
#         self.client = mqttClient.Client(str(self.id)+randomString(4))
#         self.client.on_connect=self.on_connect
#         self.client.on_publish=self.on_publish
#         # try:
#         self.client.connect(self.broker,self.port) #connect to broker
#         self.client.loop_start()
#         print("Connecting to broker:"+self.broker+":"+str(self.port))    
            
#         # except:
#         #     print(self.broker+": connection failed")
#         #     self.client.loop_stop()

# def writeLog(file,value):
#     with open("send_"+file+".txt",'a+',newline='') as f:
#         f.write('\n'+value)


# def worker(id,code,file_goal,topic,listid):   
#     comm_pubs[code] = Comm(id,broker,port,topic,file_goal,listid)
#     comm_pubs[code].connect()

# def readfile(device_num,file_goal):
#     code = randomString(3)
#     deviceid = [ [],[],[],[],[],[] ]
#     i = 0
#     for x in range(device_num):
#         deviceid[i].append(x+1)
#         i+=1
#         if i>5 :
#             i = 0
#     i = 0
#     print(deviceid)
#     for topic in topic_l:       
#         listid = deviceid[i]  
#         if len(listid)<1:
#             continue
#         id = i  
#         i=i+1     
#         p = multiprocessing.Process(target=worker, args=(id,code,file_goal,topic,listid))
#         p.start()

def on_publish(client,userdata,result): #create function for callback
    print("publish ")
    pass

def writeLog(file,value):
    with open("send_"+file+".txt",'a+',newline='') as f:
        f.write('\n'+value)

def worker(id,code,file_goal,topic,listid):
    print(datetime.now().timestamp() * 1000)    
    print("Start Simulation : "+str(id))
    i = 0
    print(client1)
    for i in range(max_rep):
        for idx in listid:
            msg = {
                "t": round(datetime.today().timestamp() * 1000)-700, #today.strftime("%Y-%m-%d %H:%M:%S"),
                "id":idx,
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

def on_connect(client, userdata, flags, rc):
    print("rc: "+str(rc))
    if rc == 0:
        deviceid = [ [],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],
                    [],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[] ]
        i = 0
        for x in range(device_num):
            deviceid[i].append(x+1)
            i+=1
            if i>34 :
                i = 0
        i = 0
        print(deviceid)
        for topic in topic_l:       
            listid = deviceid[i]  
            if len(listid)<1:
                continue
            id = i  
            i=i+1     
            # p = multiprocessing.Process(target=worker, args=(id,code,file_goal,topic,listid))
            p =  threading.Thread(target=worker, args=(id,code,file_goal,topic,listid))
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