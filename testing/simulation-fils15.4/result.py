import os, shutil
from signal import SIGABRT
import sys, json, time, datetime
import csv
import pandas as pd
import statistics
import db

db = db.dbmongo()

node = ["nu63","hl36","aa83","ty41","vb26","eu34","he48","wy38","yv34","dd52","go00","ev53","dh35","mt37","oy85"]
Simulation = 15
Max = 10000

def writeCSV(file,value):
    value = list(value.values())
    with open("feedback/feedback_"+file+".csv",'a+', newline='') as f:
        writer=csv.writer(f)
        writer.writerow(value)

def find(filename,collection,query, exclude = None, limit = None, skip = None, sort=('$natural',1)):  
    result = db.find(collection,query,exclude,limit,skip,sort)
    delay_list = []
    for item in result:
        key = item["raw_message"]["key"]
        receive = item["receive_unix_time"]
        save = item["save_unix_time"]
        delay = save - receive
        delay_list.append(delay)
        value = {
            "key" :  key,
            "receive_unix_time" : receive,
            "save_unix_time" : save,
            "delay": delay
        }
        writeCSV(filename,value)

    value = {
        'filename' :filename,
        "average_delay": averagedata(delay_list,0)
    }
    writeCSV("Total",value)  

def is_float(x):
    try:
        float(x)
    except ValueError:
        return False
    return True

def averagedata(datalist,defaultval):
    try:
        df = pd.DataFrame(datalist)
        datalist = df[df[0].apply(lambda x: is_float(x))]
        return statistics.mean(datalist[0])
    except:
        print(datalist)
        print("ERROR Average")
        return defaultval

x = 2
y = 3
for y in range(16):
    for x in range(y):
        print(node[x],"-","SIM"+str(y))
        query = {}
        query["device_code"] = node[x]
        query["raw_message.simulation"]="SIM"+str(y)    
        find("SIM"+str(y)+"-"+node[x],"sensor_data_"+node[x],query,None,Max)


