from re import S
import sys
from pymongo import collection
sys.path.append('../../')
from function import cloud9Lib
from method import fingerprint
import json 
from function import *
from controller import schemaDataController
from datetime import datetime
from datetime import timedelta
import pandas as pd
import time

prefix_collection = "schema_data_"

config_fin = {
    "lqi_threshold":89,
    "schema_dataset":"r277qj",
    "schema_goal":"u834oe",
    "waiting_time":30,#detik
    "limit_data":50 
}


##Config pakai JSON dulu.
##Baca file di db yang belum ada roomnya. urutkan natural saja
###Pakai limit bacanya
### Jika tidak data sensor yang dibaca, maka diam selama 30 detik (waiting time)
def FingerMethod(config):
    print("Start Fingerprint")
    sys.stdout.flush()
    limit =  config["limit_data"]
    goal =  prefix_collection+config["schema_goal"]
    query = {
       "room":{"$exists": False} 
    }
    result = schemaDataController.find(goal,query,None,limit)
    if result["status"]:
        result = result["data"]
        for document in result:
            fingerprint.detection(document,config)
        FingerMethod(config)
    else:
        time.sleep(config["waiting_time"])
        FingerMethod(config)


FingerMethod(config_fin)
    


