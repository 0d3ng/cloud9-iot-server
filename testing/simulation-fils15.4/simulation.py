from multiprocessing.connection import wait
import os, shutil
import sys, json, time, datetime
import random,string
import multiprocessing
from pymysql import Connect
from scipy.fftpack import ss_diff
import glob
import pandas as pd
import paho.mqtt.client as mqttClient

readPath = "dataset/"
backupPath = "dataset-log/"
broker="103.106.72.188"
port=1883
TM_wait = 30 #second
file_list = {}
minRandom = 40
maxRandom = 100

def randomString(stringLength=8):
    letters1 = string.ascii_lowercase
    letters2 = string.ascii_uppercase 
    letters3 = string.digits
    return ''.join(random.choice(letters1 + letters2 + letters3) for i in range(stringLength))


def move_file(filename,newfilename):
    src = readPath+filename
    dst = backupPath+newfilename+".csv"
    shutil.move(src,dst)

def readcsv(filename,client1):
    print("Read File "+filename+" Start")
    tstart = time.time()
    x = pd.read_csv(readPath+filename, low_memory=False)
    x = x.dropna()
    move_name = datetime.datetime.now().strftime("%Y-%m-%d %H-%M-%S %f")
    move_file(filename,"simulation-"+move_name) 
    iteration = 0
    random_val = random.randint(minRandom,maxRandom) 
    for index, row in x.iterrows():
        # print(row['topic'], row['id'], row['lq'], row['x'], row['y'], row['z'])
        try:
            topic = row['topic']
            msg = {}
            if( row['id'] != "" ):
                msg["id"] = str(int(row['id']))
            if( row['id'] != "" ):
                msg["lq"] = int(row['lq']) 
            if( row['x'] != "" and row['x'] is not None ):
                msg["x"] = float(row['x'])
            if( row['y'] != "" and row['y'] is not None ):
                msg["y"] = float(row['y'])
            if( row['z'] != "" and row['z'] is not None ):
                msg["z"] = float(row['z'])
        except:
            topic = None
        if topic :
            payload = json.dumps(msg)
            client1.publish(topic,payload=payload)
                        
        iteration+=1
        if iteration == random_val:
            time.sleep(TM_wait)
            iteration = 0
            random_val = random.randint(minRandom,maxRandom)
    tend = time.time()
    difference = tend - tstart
    print("Read File "+filename+" Stop --> "+str(difference))

def on_publish(client,userdata,result): #create function for callback
    print("data published")
    pass

def worker(filename,code):    
    client1= mqttClient.Client("simulation_"+code) #create client object
    # client1.on_publish = on_publish
    client1.connect(broker,port) #establish connection
    print("Start Simulation : "+filename+" "+code)
    readcsv(filename,client1)     

def readfile():
    for file_name in glob.glob(readPath+'*.csv'):
        file_name = os.path.basename(file_name)   
        code = randomString(3)  
        p = multiprocessing.Process(target=worker, args=(file_name,code))
        p.start()

if __name__ == '__main__':    
    while True:
        readfile()
        time.sleep(10)