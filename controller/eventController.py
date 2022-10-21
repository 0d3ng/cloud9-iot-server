#!/usr/bin/python3

import sys
from bson import ObjectId
import json 
from function import *
import datetime
import pandas as pd
from controller import sensorController
from pytz import timezone
import statistics

from configparser import ConfigParser

from function import cloud9Lib
config = ConfigParser()
config.read("config.ini")

sensors = []
db = db.dbmongo()
td = datetime.timedelta(hours=int(config["SERVER"]["timediff"]))

collection = "event"
prefix_collection_sensor = "sensor_data_"
prefix_collection_schema = "schema_data_"

def add(fillData):  
    insertQuery = {
        'event_code':fillData.get('event_code', None),
        'device':fillData.get('device', None),
        'name':fillData.get('name', None),
        'active':fillData.get('active', False),
        'rules':fillData.get('rules', None), #json object for rules
        'action':fillData.get('action', None), #json object for actions
        'date_add': datetime.datetime.utcnow(),
        'add_by':fillData.get('add_by', None),
        'event_sleep_time':fillData.get('event_sleep_time', None), #in second
        'last_event':fillData.get('last_event', None), #array[detail, purpose]        
    }
    result = db.insertData(collection,insertQuery)
    if result == []:
        response = {'status':False, 'message':"Add Failed"}               
    else:
        response = {'status':True,'message':'Success','data':result}
        if insertQuery['active'] == True:
            triggerService(insertQuery["event_code"],True)
    return cloud9Lib.jsonObject(response)

def find(query):  
    result = db.find(collection,query)
    if result == []:
        response = {"status":False, "data":query}               
    else:
        response = {'status':True, 'data':result}    
    return cloud9Lib.jsonObject(response)

def findOne(query):  
    result = db.findOne(collection,query, None)
    if result is None or result is False:
        response = {"status":False, "data":query}               
    else:
        response = {'status':True,'message':'Success','data':result}    
    return cloud9Lib.jsonObject(response)

def update(query,data):            
    updateData = {}
    queryUpdate = {}

    if 'event_code' in query: queryUpdate['event_code'] = query['event_code']
    if '_id' in query: queryUpdate['_id'] = query['_id']
    
    if 'name' in data: updateData['name'] = data['name']
    if 'active' in data: updateData['active'] = data['active']
    if 'event_code' in data: updateData['event_code'] = data['event_code']
    if 'device' in data: updateData['device'] = data['device']
    if 'rules' in data: updateData['rules'] = data['rules']
    if 'action' in data: updateData['action'] = data['action']
    if 'event_sleep_time' in data: updateData['event_sleep_time'] = data['event_sleep_time']
    if 'last_event' in data: updateData['last_event'] = data['last_event']
    if 'updated_by' in data: updateData['updated_by'] = data['updated_by']

    if updateData == []:
        return {"status":False, "message":"UPDATE NONE"}   
    last = findOne(queryUpdate)['data'] 
    # print(updateData)
    # print(last)
    # sys.stdout.flush()    
    result = db.updateData(collection,queryUpdate,updateData)
    if not result :
        response = {"status":False, "message":"UPDATE FAILED"}               
    else:
        response = {'status':True,'message':'Success','data':result}
        if last['active'] !=  updateData['active']:
            if updateData['active'] == True:
                triggerService(last["event_code"],True)
            if updateData['active'] == False:
                triggerService(last["event_code"],False)
    return cloud9Lib.jsonObject(response)

def delete(query): 
    listData = findOne(query)['data']           
    result = db.deleteData(collection,query)
    if not result:
        response = {"status":False, "message":"DELETE FAILED"}               
    else:
        response = {'status':True,'message':'Success','data':result}
        triggerService(listData["event_code"],False)
    return cloud9Lib.jsonObject(response)

def triggerService(event_code,status):
    send = {
        "event_code":event_code
    }
    print("----------------------")
    print(send)
    if status :
        print(config["MQTT"]["event_stream_start"])
        mqttcom.publish(config["MQTT"]["event_stream_start"],send)
        return
    else:
        print(config["MQTT"]["event_stream_stop"])
        mqttcom.publish(config["MQTT"]["event_stream_stop"],send)
        return


def getSensorData(time_str,time_end,code,key,value,collectid=None):
    if(collectid):
        collection = prefix_collection_sensor+str(collectid)
    else:
        collection = prefix_collection_sensor+str(code)
    datesrc_str = datetime.datetime.strptime(time_str,'%Y-%m-%d %H:%M:%S') - td
    datesrc_end = datetime.datetime.strptime(time_end,'%Y-%m-%d %H:%M:%S') - td
    query = {
        'date_add_server': {"$gte": datesrc_str,"$lt": datesrc_end},
        "$and":[{str(key):{"$ne":None}},{str(key):{"$ne":""}}]
    }    
    if(collectid):
        query["device_code"] = str(code)        
    
    group = {
        "_id":"$"+str(key)                
    }
    for item in value:
        group[str(item)] = {"$push":"$"+str(item)}

    pipeline = [{"$unwind":"$"+str(key) },
            {"$match":query},
            {"$sort" : { 'date_add_server' : -1 }},
            {"$group":group}]
    # print("--------------------")
    # print(collection)
    # print(pipeline)    
    result = db.aggregate(collection,pipeline)
    response = {}
    itemkey = []
    for item in result:
        response[item[str(key)]] = item
        itemkey.append(item[str(key)])
    # print(response)
    # print("-------++++---------")
    sys.stdout.flush()
    return response,itemkey