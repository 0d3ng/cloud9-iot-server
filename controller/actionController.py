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

collection = "action"
prefix_collection_sensor = "sensor_data_"
prefix_collection_schema = "schema_data_"

def add(fillData):  
    insertQuery = {
        'action_code':fillData.get('action_code', None),
        'name':fillData.get('name', None),
        'type':fillData.get('type', None),
        'configuration':fillData.get('configuration', None), #json object for rules
        'date_add': datetime.datetime.utcnow(),
        'add_by':fillData.get('add_by', None)
    }
    result = db.insertData(collection,insertQuery)
    if result == []:
        response = {'status':False, 'message':"Add Failed"}               
    else:
        response = {'status':True,'message':'Success','data':result}
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

    if 'action_code' in query: queryUpdate['action_code'] = query['action_code']
    if '_id' in query: queryUpdate['_id'] = query['_id']
    
    if 'name' in data: updateData['name'] = data['name']
    if 'action_code' in data: updateData['action_code'] = data['action_code']
    if 'type' in data: updateData['type'] = data['type']
    if 'configuration' in data: updateData['configuration'] = data['configuration']    
    if 'updated_by' in data: updateData['updated_by'] = data['updated_by']

    if updateData == []:
        return {"status":False, "message":"UPDATE NONE"}   
    result = db.updateData(collection,queryUpdate,updateData)
    if not result :
        response = {"status":False, "message":"UPDATE FAILED"}               
    else:
        response = {'status':True,'message':'Success','data':result}        
    return cloud9Lib.jsonObject(response)

def delete(query): 
    listData = findOne(query)['data']           
    result = db.deleteData(collection,query)
    if not result:
        response = {"status":False, "message":"DELETE FAILED"}               
    else:
        response = {'status':True,'message':'Success','data':result}        
    return cloud9Lib.jsonObject(response)
