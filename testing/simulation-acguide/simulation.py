from multiprocessing.connection import wait
import os, shutil
import sys, json, time, datetime
import random,string
import multiprocessing
from pymysql import Connect
from scipy.fftpack import ss_diff
import glob
import pandas as pd
from pytz import timezone
import time
import paho.mqtt.client as mqttClient

readPath = "dataset/"
backupPath = "dataset-log/"
broker= "localhost"#"103.106.72.188"#
topic="acguide/d207"
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
        print(row['co2_value'], row['window_state'], row['door_state'], row['person_num'], row['ventilate_state'], row['temperature_in'], row['humidity_in'], row['di_in'], row['ac_state']) 
        try:            
            msg = {}
            if( row['co2_value'] != "" ):
                msg["co2_value"] = int(row['co2_value'])
            if( row['window_state'] != "" ):
                msg["window_state"] = int(row['window_state'])
            if( row['door_state'] != "" ):
                msg["door_state"] = int(row['door_state'])
            if( row['person_num'] != "" ):
                msg["person_num"] = int(row['person_num'])
            if( row['ventilate_state'] != "" ):
                msg["ventilate_state"] = int(row['ventilate_state'])
            if( row['temperature_in'] != "" ):
                msg["temperature_in"] = float(row['temperature_in'])
            if( row['humidity_in'] != "" ):
                msg["humidity_in"] = float(row['humidity_in'])
            if( row['di_in'] != "" ):
                msg["di_in"] = float(row['di_in'])
            if( row['ac_state'] != "" ):
                msg["ac_state"] = str(row['ac_state']) 
            send = True
        except:
            send = False
        if send :
            msg["date_add_sensor"] =  round(datetime.datetime.now(datetime.timezone.utc).timestamp()*1000) #round(datetime.datetime.now(timezone('Asia/Tokyo')).timestamp() * 1000)
            payload = json.dumps(msg)
            client1.publish(topic,payload=payload)
            time.sleep(TM_wait)   
        else:
            time.sleep(5)  
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