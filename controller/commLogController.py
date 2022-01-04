#!/usr/bin/python3

import sys
from bson import ObjectId
import json 
from function import *
import datetime


sensors = []
db = db.dbmongo()
prefix = "sensor_col_"
collection = "communication_log"

def add(fillData):  
    insertQuery = {
        'token_access':fillData.get('token_access', None),
        'ip_sender':fillData.get('ip_sender', None), 
        'topic':fillData.get('topic', None), 
        'channel_type':fillData.get('channel_type', None),
        'raw_message':fillData.get('raw_message', None),
        'date_server': datetime.datetime.utcnow()        
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
    if result is None:
        response = {"status":False, "data":query}               
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

def averageData(datalist):
    for key in datalist:
        datalist[key] = sum(datalist[key]) / len(datalist[key])
    return datalist 

def grouping(datalist):
    d = {}
    for key, val in datalist:
        if not key  in d:
            d[key] = []
        d[key].append(val)
    return d 