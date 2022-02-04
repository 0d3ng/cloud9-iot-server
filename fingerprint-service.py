import sys
from function import cloud9Lib
from method import *
import json 
from function import *
from controller import schemaDataController
from datetime import datetime
from datetime import timedelta
import pandas as pd
import time

prefix_collection = "schema_data_"
waiting_time_default = 10

# config_fin = {
#     "lqi_threshold":89,
#     "schema_dataset":"r277qj",
#     "schema_goal":"u834oe",
#     "waiting_time":30,#detik
#     "limit_data":50 
# }
def FingerMethod():
    fingerconfig = schemaDataController.findOne("analytical_service",{"method":"fingerprint"})    
    if(fingerconfig["status"]):
        try:
            config = fingerconfig["data"]["config"]
            limit =  int(config["limit_data"])
            goal =  prefix_collection+config["schema_goal"]
            query = {
            "room":{"$exists": False} 
            }
            result = schemaDataController.find(goal,query,None,limit)
            if result["status"]:
                result = result["data"]
                for document in result:
                    fingerprint.detection(document,config)
                FingerMethod()
            else:
                time.sleep(config["waiting_time"])
                FingerMethod()
        except Exception as e:
            print("-------Error---------")
            print(e)
            time.sleep(waiting_time_default)
            FingerMethod()
    else :
        time.sleep(waiting_time_default)
        FingerMethod()


print("Start Fingerprint Method")
sys.stdout.flush()
FingerMethod()
