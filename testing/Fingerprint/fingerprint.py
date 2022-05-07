
import sys
from bson import ObjectId
import json 
import datetime
import pandas as pd
from pytz import timezone
import statistics
import math
import requests
import time

prefix_collection = "schema_data_"
waiting_time_default = 10

config_fin = {
    "lqi_threshold":89,
    "schema_dataset":"xp10lj",
    "schema_goal":"5y76py",
    "waiting_time":30,#detik
    "limit_data":20, 
    "rest_api":"http://103.106.72.188:3001/schema/data/"
}

def detection(testData,lastData,fingerprint_data,config):
    lqDroppedIdentifier = 0
    detectRoom = "-"
    LQI_threshold = config["lqi_threshold"]
    fingerprint_collection = prefix_collection+config["schema_dataset"]
    data_collection = prefix_collection+config["schema_goal"]
    variance1 = testData['variance1']
    variance2 = testData['variance2']
    variance3 = testData['variance3']
    variance4 = testData['variance4']
    variance5 = testData['variance5']
    variance6 = testData['variance6']
    accelero_data = max(testData['accelvariance1'],testData['accelvariance2'],testData['accelvariance3'],testData['accelvariance4'],testData['accelvariance5'],testData['accelvariance6'])
    id = testData['id']
    _id = testData['_id']


    if(testData['lq1'] > LQI_threshold):
        query = {
            'lq1':{"$gt": 90}
        }        
    elif(testData['lq2'] > LQI_threshold):
        query = {
            'lq2':{"$gt": 90}
        } 
    elif(testData['lq3'] > LQI_threshold):
        query = {
            'lq3':{"$gt": 90}
        } 
    elif(testData['lq4'] > LQI_threshold):
        query = {
            'lq4':{"$gt": 90}
        }
    elif(testData['lq5'] > LQI_threshold):
        query = {
            'lq5':{"$gt": 90}
        }
    elif(testData['lq6'] > LQI_threshold):
        query = {
            'lq6':{"$gt": 90}
        }
    else:
        query = {}
    
    # fingerprint_data = schemaDataController.find(fingerprint_collection,query)["data"]

    if variance1 > 80 or variance2 > 80 or variance3 > 80 or variance4 > 80 or variance5 > 80 or variance6 > 80:
        if lqDroppedIdentifier == 0:
            if variance1 > 80 and testData['lq1'] == 5: 
               lqDroppedIdentifier = 1
            if variance1 > 80 and testData['lq2'] == 5: 
               lqDroppedIdentifier = 2
            if variance1 > 80 and testData['lq3'] == 5: 
               lqDroppedIdentifier = 3
            if variance1 > 80 and testData['lq4'] == 5: 
               lqDroppedIdentifier = 4
            if variance1 > 80 and testData['lq5'] == 5: 
               lqDroppedIdentifier = 5
            if variance1 > 80 and testData['lq6'] == 5: 
               lqDroppedIdentifier = 6
        
        try:
            print(id+" --> [LQ VAR > 80]")
            if accelero_data < 150:
                if lqDroppedIdentifier != 0:
                    if testData['lq{}'.format(lqDroppedIdentifier)] == 5:
                        if( id in lastData):
                            LastRoom = lastData[id]
                        else:
                            LastRoom = "-"
                        detectRoom = LastRoom
                    else:
                        lqDroppedIdentifier = 0
                        euc_dist = []
                        room = []
                        print(id+" --> [ACCELERO > 150] : Process recent data with euclidean distance")
                        for j in fingerprint_data:
                            lqi1 = round(testData['lq1'], 2) - float(j["lq1"])
                            lqi2 = round(testData['lq2'], 2) - float(j["lq2"])
                            lqi3 = round(testData['lq3'], 2) - float(j["lq3"])
                            lqi4 = round(testData['lq4'], 2) - float(j["lq4"])
                            lqi5 = round(testData['lq5'], 2) - float(j["lq5"])
                            lqi6 = round(testData['lq6'], 2) - float(j["lq6"])
                            dist = (pow(lqi1, 2)+pow(lqi2, 2)+pow(lqi3, 2)+
                            pow(lqi4, 2)+pow(lqi5, 2)+pow(lqi6, 2))
                            x_euc_dist = round(math.sqrt(dist), 2)
                            euc_dist.append(x_euc_dist)
                            room.append(j['room'])
                        max_euc_dist = euc_dist.index(min(euc_dist))
                        detectRoom = room[max_euc_dist]
                else:
                    ##Get Room Before
                    if( id in lastData):
                        LastRoom = lastData[id]
                    else:
                        LastRoom = "-"
                    detectRoom = LastRoom   
            
            if accelero_data >= 150:   
                lqDroppedIdentifier = 0
                euc_dist = []
                room = []
                print(id+" --> [ACCELERO > 150] : Process recent data with euclidean distance")
                for j in fingerprint_data:
                    lqi1 = round(testData['lq1'], 2) - float(j["lq1"])
                    lqi2 = round(testData['lq2'], 2) - float(j["lq2"])
                    lqi3 = round(testData['lq3'], 2) - float(j["lq3"])
                    lqi4 = round(testData['lq4'], 2) - float(j["lq4"])
                    lqi5 = round(testData['lq5'], 2) - float(j["lq5"])
                    lqi6 = round(testData['lq6'], 2) - float(j["lq6"])
                    dist = (pow(lqi1, 2)+pow(lqi2, 2)+pow(lqi3, 2)+
                    pow(lqi4, 2)+pow(lqi5, 2)+pow(lqi6, 2))
                    x_euc_dist = round(math.sqrt(dist), 2)
                    euc_dist.append(x_euc_dist)
                    room.append(j['room'])
                max_euc_dist = euc_dist.index(min(euc_dist))
                detectRoom = room[max_euc_dist]

        except KeyError:
            print(id+" --> KeyError in ValueChecker")
            euc_dist = []
            room = []
            print(id+" --> [ACCELERO > 150] : Process recent data with euclidean distance")
            for j in fingerprint_data:
                lqi1 = round(testData['lq1'], 2) - float(j["lq1"])
                lqi2 = round(testData['lq2'], 2) - float(j["lq2"])
                lqi3 = round(testData['lq3'], 2) - float(j["lq3"])
                lqi4 = round(testData['lq4'], 2) - float(j["lq4"])
                lqi5 = round(testData['lq5'], 2) - float(j["lq5"])
                lqi6 = round(testData['lq6'], 2) - float(j["lq6"])
                dist = (pow(lqi1, 2)+pow(lqi2, 2)+pow(lqi3, 2)+
                pow(lqi4, 2)+pow(lqi5, 2)+pow(lqi6, 2))
                x_euc_dist = round(math.sqrt(dist), 2)
                euc_dist.append(x_euc_dist)
                room.append(j['room'])
            max_euc_dist = euc_dist.index(min(euc_dist))
            detectRoom = room[max_euc_dist]
    
    else:
        euc_dist = []
        room = []
        for j in fingerprint_data:
            lqi1 = round(testData['lq1'], 2) - float(j["lq1"])
            lqi2 = round(testData['lq2'], 2) - float(j["lq2"])
            lqi3 = round(testData['lq3'], 2) - float(j["lq3"])
            lqi4 = round(testData['lq4'], 2) - float(j["lq4"])
            lqi5 = round(testData['lq5'], 2) - float(j["lq5"])
            lqi6 = round(testData['lq6'], 2) - float(j["lq6"])
            dist = (pow(lqi1, 2)+pow(lqi2, 2)+pow(lqi3, 2)+
            pow(lqi4, 2)+pow(lqi5, 2)+pow(lqi6, 2))
            x_euc_dist = round(math.sqrt(dist), 2)
            euc_dist.append(x_euc_dist)
            room.append(j['room'])
        max_euc_dist = euc_dist.index(min(euc_dist))
        detectRoom = room[max_euc_dist]
    msg = {
        "_id":_id,
        "room":detectRoom,
        "date_detection":datetime.datetime.now(timezone('Asia/Tokyo')).strftime("%Y-%m-%d %H:%M:%S")
    }
    try:
        update = HTTP_post(config["rest_api"]+config["schema_goal"]+"/edit/",msg)
        time.sleep(3)
        print(msg)
        print(update)
        if(update['status']):
            return 1
        else:
            return 0
    except Exception as e:
        print(e)
        return 0

def HTTP_post(url,message):
    payload = json.dumps(message)
    headers = {
        'Content-Type': 'application/json'
    }
    response = requests.request("POST", url, headers=headers, data = payload)
    return json.loads(response.text.encode('utf8'))

def FingerMethod():
    # try:
        config = config_fin
        dataSet = HTTP_post(config["rest_api"]+config["schema_dataset"],{})        
        if dataSet["status"] : 
            dataSet = dataSet["data"]
            query = {
                "groupby":"id",
                "query":{
                    "$and":[
                        {"room":{"$exists": True}},
                        {"room":{"$nin": ["","-",None]}}
                    ]
                    
                },
                "field":{
                    "room":"room"
                }
            }
            lastData = {}
            getLastData = HTTP_post(config["rest_api"]+config["schema_goal"]+"/group",query)
            print(getLastData)
            if getLastData["status"] :
                for item  in getLastData["data"]:
                    lastData[item["id"]] = item["room"]
            limit =  int(config["limit_data"])
            query = {
                "$or":[
                    {"room":{"$exists": False}},
                    {"room":""},
                    {"room":"-"},
                    {"room":None}
                ],
                # "sort":{
                #     "field":"date_add_auto",
                #     "type":1
                # },
                "limit":limit
            }
            dataTest = HTTP_post(config["rest_api"]+config["schema_goal"],query)
            if dataTest["status"]:
                dataTest = dataTest["data"]
                for document in dataTest:
                    detection(document,lastData,dataSet,config)
                FingerMethod()
            else:
                time.sleep(config["waiting_time"])
                FingerMethod()
        else:
            dataSet = []
            print("Dataset not found")        
    # except Exception as e:
    #     print("-------Error---------")
    #     print(e)
    #     time.sleep(waiting_time_default)
    #     FingerMethod()
    

print("Start Fingerprint Method")
sys.stdout.flush()
FingerMethod()
# dataSet = HTTP_post(config_fin["rest_api"]+config_fin["schema_dataset"],{}) 
# print(dataSet["data"])