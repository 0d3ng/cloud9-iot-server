import sys
from bson import ObjectId
import json 
from function import *
import datetime
import pandas as pd
from controller import schemaDataController
from controller import sensorController
from pytz import timezone
import numpy as np

from configparser import ConfigParser

from function import cloud9Lib
config = ConfigParser()
config.read("config.ini")

sensors = []
db = db.dbmongo()
td = datetime.timedelta(hours=int(config["SERVER"]["timediff"]))

collection = "interface"
prefix_collection_sensor = "sensor_data_"
prefix_collection_schema = "schema_data_"

def add(fillData):  
    insertQuery = {
        'interface_code':fillData.get('interface_code', None),        
        'type':fillData.get('type', None),
        'title':fillData.get('tiltle', None),
        'resource':fillData.get('resource', None),
        'resource_code':fillData.get('resource_code', None),
        'configuration':fillData.get('configuration', None), 
        'date_add': datetime.datetime.utcnow(),
        'add_by':fillData.get('add_by', None),
    }
    result = db.insertData(collection,insertQuery)
    if result == []:
        response = {'status':False, 'message':"Add Failed"}               
    else:
        response = {'status':True,'message':'Success','data':result}
        if insertQuery['stream'] == True:
            triggerService(insertQuery["interface_code"],True)
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
    if 'interface_code' in query: queryUpdate['interface_code'] = query['interface_code']
    if '_id' in query: queryUpdate['_id'] = query['_id']

    if 'interface_code' in data: updateData['interface_code'] = data['interface_code']
    if 'type' in data: updateData['type'] = data['type']
    if 'title' in data: updateData['title'] = data['title']
    if 'resource' in data: updateData['resource'] = data['resource']
    if 'resource_code' in data: updateData['resource_code'] = data['resource_code']
    if 'configuration' in data: updateData['configuration'] = data['configuration']

    if updateData == []:
        return {"status":False, "message":"UPDATE NONE"}   
    last = findOne(queryUpdate)['data'] 
        
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