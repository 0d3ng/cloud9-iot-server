#!/usr/bin/python3

import sys
from bson import ObjectId
import json 
from function import *



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

def findOne(collection,query, exclude = None, limit = None, skip = None, sort=('$natural',1)):  
    result = db.findOne(collection,query,exclude,limit,skip,sort)
    if result == []:
        response = {"status":False, "data":query}               
    else:
        response = {"status":True, 'message':'Success','data':result}
    return cloud9Lib.jsonObject(response)

def update(collection,query,data):    
    if data == []:
        return {"status":False, "message":"UPDATE NONE"}    
    result = db.updateData(collection,query,query)
    if result == []:
        response = {"status":False, "message":"UPDATE FAILED"}               
    else:
        response = {'status':True,'message':'Success','data':result}
    return cloud9Lib.jsonObject(response)

def delete(collection,id):        
    query = {"_id":ObjectId(id)}
    result = db.deleteData(collection,query)
    if result == []:
        response = {"status":False, "message":"DELETE FAILED"}               
    else:
        response = {'status':True,'message':'Success','data':result}
    return cloud9Lib.jsonObject(response)