#!/usr/bin/python3

import sys
from bson import ObjectId
import json 
from function import *

sensors = []
db = db.dbmongo()

collection = "log"

def add(data):    
    result = db.insertData(collection,data)
    if result == []:
        response = {"status":False, "message":"Add Failed"}               
    else:
        response = {'status':True,'message':'Success','data':result}
    return response

def find():  
    result = db.find(collection,query,{})
    if result == []:
        response = {"status":False, "data":result}               
    else:
        response = {'status':True, 'data':result}
    return response

def findOne(query):  
    result = db.findOne(collection,query,{})
    if result == []:
        response = {"status":False, "data":result}               
    else:
        response = {'message':'Success','list':result}
    return response

def update(query,data):    
    if data == []:
        return {"status":False, "message":"UPDATE NONE"}    
    result = db.updateData(collection,query,data)
    if result == []:
        response = {"status":False, "message":"UPDATE FAILED"}               
    else:
        response = {'status':True,'message':'Success','data':result}
    return response

def delete(id):        
    query = {"_id":ObjectId(id)}
    result = db.deleteData(collection,query)
    if result == []:
        response = {"status":False, "message":"DELETE FAILED"}               
    else:
        response = {'status':True,'message':'Success','data':result}
    return response