#!/usr/bin/python3

import sys
from bson import ObjectId
import json 
from function import *
import datetime
import pandas as pd
from controller import schemaDataController
from controller import sensorController
from pytz import timezone
import statistics
import math

prefix_collection = "schema_data_"

def detection(testData,config):
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
    try:
        _id = ObjectId(testData['_id'])
    except:
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
    
    fingerprint_data = schemaDataController.find(fingerprint_collection,query)["data"]

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
                        ##Get Room Before
                        query = { 'id' : id }
                        Lastdata = schemaDataController.findOne(data_collection,query,None,('date_add_auto',-1))
                        if(Lastdata['status']):
                            LastRoom = Lastdata['data']['room']
                        else:
                            LastRoom = "-"
                        detectRoom = LastRoom
                    else:
                        lqDroppedIdentifier = 0
                        euc_dist = []
                        room = []
                        print(id+" --> [ACCELERO > 150] : Process recent data with euclidean distance")
                        for j in fingerprint_data:
                            lqi1 = testData['lq1'] - j["lq1"]
                            lqi2 = testData['lq2'] - j["lq2"]
                            lqi3 = testData['lq3'] - j["lq3"]
                            lqi4 = testData['lq4'] - j["lq4"]
                            lqi5 = testData['lq5'] - j["lq5"]
                            lqi6 = testData['lq6'] - j["lq6"]
                            dist = (pow(lqi1, 2)+pow(lqi2, 2)+pow(lqi3, 2)+
                            pow(lqi4, 2)+pow(lqi5, 2)+pow(lqi6, 2))
                            x_euc_dist = round(math.sqrt(dist), 2)
                            euc_dist.append(x_euc_dist)
                            room.append(j['room'])
                        max_euc_dist = euc_dist.index(min(euc_dist))
                        detectRoom = room[max_euc_dist]
                else:
                    ##Get Room Before
                    query = { 'id' : id }
                    Lastdata = schemaDataController.findOne(data_collection,query,None,('date_add_auto',-1))
                    if(Lastdata['status']):
                        LastRoom = Lastdata['data']['room']
                    else:
                        LastRoom = "-"
                    detectRoom = LastRoom   
            
            if accelero_data >= 150:   
                lqDroppedIdentifier = 0
                euc_dist = []
                room = []
                print(id+" --> [ACCELERO > 150] : Process recent data with euclidean distance")
                for j in fingerprint_data:
                    lqi1 = testData['lq1'] - j["lq1"]
                    lqi2 = testData['lq2'] - j["lq2"]
                    lqi3 = testData['lq3'] - j["lq3"]
                    lqi4 = testData['lq4'] - j["lq4"]
                    lqi5 = testData['lq5'] - j["lq5"]
                    lqi6 = testData['lq6'] - j["lq6"]
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
                lqi1 = testData['lq1'] - j["lq1"]
                lqi2 = testData['lq2'] - j["lq2"]
                lqi3 = testData['lq3'] - j["lq3"]
                lqi4 = testData['lq4'] - j["lq4"]
                lqi5 = testData['lq5'] - j["lq5"]
                lqi6 = testData['lq6'] - j["lq6"]
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
            lqi1 = testData['lq1'] - j["lq1"]
            lqi2 = testData['lq2'] - j["lq2"]
            lqi3 = testData['lq3'] - j["lq3"]
            lqi4 = testData['lq4'] - j["lq4"]
            lqi5 = testData['lq5'] - j["lq5"]
            lqi6 = testData['lq6'] - j["lq6"]
            dist = (pow(lqi1, 2)+pow(lqi2, 2)+pow(lqi3, 2)+
            pow(lqi4, 2)+pow(lqi5, 2)+pow(lqi6, 2))
            x_euc_dist = round(math.sqrt(dist), 2)
            euc_dist.append(x_euc_dist)
            room.append(j['room'])
        max_euc_dist = euc_dist.index(min(euc_dist))
        detectRoom = room[max_euc_dist]
    
    query = { "_id":_id }
    updateData = {"room":detectRoom,"date_detection":datetime.datetime.now(timezone('Asia/Tokyo'))}
    update = schemaDataController.update(data_collection,query,updateData)
    if(update['status']):
        return 1
    else:
        return 0



