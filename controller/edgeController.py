#!/usr/bin/python3

import sys
from bson import ObjectId
import json 
from function import *
from datetime import datetime,timedelta
from controller import comChannelController
from pytz import timezone

sensors = []
db = db.dbmongo()

collection = "edgeconfig"

def add(fillData):  
    insertQuery = {
        'edgeconfig_code':fillData.get('edgeconfig_code', None),
        'device_code':fillData.get('device_code', None),
        'method':fillData.get('method', None),
        'string_sample':fillData.get('string_sample', None),
        'delimeter':fillData.get('delimeter', None), #arraylist [dem1,dem2]
        'string_pattern':fillData.get('string_pattern', None),
        'object_used':fillData.get('string_pattern', None),
        'active':fillData.get('active', False),                        
        'date_add': datetime.utcnow(),
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
    if 'edgeconfig_code' in query: queryUpdate['edgeconfig_code'] = query['edgeconfig_code']
    if '_id' in query: queryUpdate['_id'] = query['_id']
    
    if 'device_code' in data: updateData['device_code'] = data['device_code']
    if 'method' in data: updateData['method'] = data['method']
    if 'string_sample' in data: updateData['string_sample'] = data['string_sample']
    if 'delimeter' in data: updateData['delimeter'] = data['delimeter']
    if 'string_pattern' in data: updateData['string_pattern'] = data['string_pattern']
    if 'object_used' in data: updateData['object_used'] = data['object_used']
    if 'active' in data: updateData['active'] = data['active']
    if 'updated_by' in data: updateData['updated_by'] = data['updated_by']
    if 'date_download' in data: 
        try:
            updateData['date_download'] = cloud9Lib.cv2datetime(data['date_download'])
        except:
            updateData['date_download'] = datetime.now(timezone('Asia/Tokyo'))

    if updateData == []:
        return {"status":False, "message":"UPDATE NONE"}        
    result = db.updateData(collection,queryUpdate,updateData)
    if not result :
        response = {"status":False, "message":"UPDATE FAILED"}               
    else:
        response = {'status':True,'message':'Success','data':result}
    return cloud9Lib.jsonObject(response)

def delete(query):            
    result = db.deleteData(collection,query)
    if not result:
        response = {"status":False, "message":"DELETE FAILED"}               
    else:
        response = {'status':True,'message':'Success','data':result}
    return cloud9Lib.jsonObject(response)


#====================================================================


from detect_delimiter import detect

def detect_delimeter(raw):
    raw = str(raw)
    raw = raw.strip()
    delimeter = detect(raw)
    return delimeter

def covert_to_list(raw,delimeter=None):
    raw = str(raw)
    raw = raw.strip()
    if(delimeter == None):
        delimeter = detect_delimeter(raw)  
        if(delimeter == None):            
            return False,None                      
    x = raw.split(delimeter)
    return x,delimeter

def convert_to_json(raw,delimeter1=None,delimeter2=None): #only 2 dimension array
    item,dem = covert_to_list(raw,delimeter1)
    if(item == False):
        return False,delimeter1,delimeter2
    json_object = {}
    for i in item:
        if len(i)<1:
            continue
        value,dem2 = covert_to_list(i,delimeter2)
        if(value == False):
            continue
        if(len(value)>1):
            json_object[ value[0] ] = value[1]
    return json_object,dem,dem2 