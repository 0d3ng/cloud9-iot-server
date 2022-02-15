#!/usr/bin/python3

import sys
from bson import ObjectId
import json 
from function import *
from controller import schemaController
from datetime import datetime
from pytz import timezone

prefix_collection = "schema_data_"

sensors = []
db = db.dbmongo()
def add(collection,fillData):    
    result = db.insertData(collection,fillData)
    if result == []:
        response = {'status':False, 'message':"Add Failed"}               
    else:
        response = {'status':True,'message':'Success','data':result}
    return cloud9Lib.jsonObject(response)

def find(collection,query, exclude = None, limit = None, skip = None, sort=('$natural',1)):  
    result = db.find(collection,query,exclude,limit,skip,sort)
    if result == []:
        response = {"status":False, "data":query}               
    else:
        response = {'status':True, 'data':result}    
    return cloud9Lib.jsonObject(response)

def findOne(collection,query, exclude = None, sort=('$natural',1)):      
    result = db.findOne(collection,query,exclude,sort)
    if result == [] or result == False:
        response = {"status":False, "data":query}               
    else:
        response = {"status":True, 'message':'Success','data':result}
    return cloud9Lib.jsonObject(response)

def update(collection,query,data):    
    if data == []:
        return {"status":False, "message":"UPDATE NONE"}    
    result = db.updateData(collection,query,data)
    if not result :
        response = {"status":False, "message":"UPDATE FAILED"}               
    else:
        response = {'status':True,'message':'Success','data':result}
    return cloud9Lib.jsonObject(response)

def delete(collection,query):        
    result = db.deleteData(collection,query)
    if not result:
        response = {"status":False, "message":"DELETE FAILED"}               
    else:
        response = {'status':True,'message':'Success','data':result}
    return cloud9Lib.jsonObject(response)


def filter(schema_code,value):
    query = {
        'schema_code' : schema_code
    }
    schemaData = schemaController.findOne(query)
    if schemaData['status'] == True :
        data = {}
        schemaData = schemaData['data']['field']
        for fieldData in schemaData:
            fieldName = list(fieldData.keys())[0]
            fieldType = fieldData[fieldName]
            if fieldName in value:
                if fieldType == "int":
                    # val = int(value[fieldName])
                    val = round(float(value[fieldName]))
                elif fieldType == "float":
                    val = float(value[fieldName])
                elif fieldType == "str":
                    val = str(value[fieldName])
                elif fieldType == "boolean":
                    val = cloud9Lib.str2bool(value[fieldName])
                elif fieldType == "datetime":
                    val = cloud9Lib.cv2datetime(value[fieldName])
                elif fieldType == "date":
                    val = cloud9Lib.cv2date(value[fieldName])
                elif fieldType == "time":
                    val = cloud9Lib.cv2time(value[fieldName])
                data[fieldName] = val        
        
        if 'date_detection' in value:
            data['date_detection'] = cloud9Lib.cv2datetime(value['date_detection'])
        response = {'status':True, 'data':data}
    else:
        response = {'status':False, 'message':"Add Failed"}
    return cloud9Lib.jsonObject(response)

def filterAdd(schema_code,value):
    query = {
        'schema_code' : schema_code
    }
    schemaData = schemaController.findOne(query)
    if schemaData['status'] == True :
        data = {}
        schemaData = schemaData['data']['field']
        print(schemaData)
        print("-------------------------")
        sys.stdout.flush()
        for fieldData in schemaData:
            fieldName = list(fieldData.keys())[0]
            fieldType = fieldData[fieldName]
            if fieldName in value:
                if fieldType == "int":
                    val = int(value[fieldName])
                elif fieldType == "float":
                    val = float(value[fieldName])
                elif fieldType == "str":
                    val = str(value[fieldName])
                elif fieldType == "boolean":
                    val = cloud9Lib.str2bool(value[fieldName])
                elif fieldType == "datetime":
                    val = cloud9Lib.cv2datetime(value[fieldName])
                elif fieldType == "date":
                    val = cloud9Lib.cv2date(value[fieldName])
                elif fieldType == "time":
                    val = cloud9Lib.cv2time(value[fieldName])
                print(fieldName)
                print(val)
                data[fieldName] = val
        data["date_add_auto"] = datetime.now(timezone('Asia/Tokyo'))
        insert = add(prefix_collection+schema_code,data)
        if insert['status']:
            response = {'status':True, 'message':"Success"}
        else:
            response = {'status':False, 'message':"Add Failed"}
    else:
        response = {'status':False, 'message':"Add Failed"}
    print(response)
    sys.stdout.flush()
    return cloud9Lib.jsonObject(response)

def aggregate(collection,pipeline):  
    result = db.aggregate(collection,pipeline)
    if result == []:
        response = {"status":False, "data":pipeline}               
    else:
        response = {'status':True, 'data':result}    
    return cloud9Lib.jsonObject(response)
