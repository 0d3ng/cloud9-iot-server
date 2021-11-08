#!/usr/bin/python3

import sys
from bson import ObjectId
import json 
from function import *
import datetime


sensors = []
db = db.dbmongo()

collection = "user"

def add(fillData):  
    insertQuery = {
        'username':fillData.get('username', None),
        'password':fillData.get('password', None),
        'email':fillData.get('email', None), 
        'name':fillData.get('name', None),
        'otp':fillData.get('otp', None), 
        'expired_otp':fillData.get('expired_otp', None),
        'add_by':fillData.get('add_by', None),
        'date_add': datetime.datetime.utcnow(),
        'active':fillData.get('active', False) 
    }
    result = db.insertData(collection,insertQuery)
    if result == []:
        response = {'status':False, 'message':"Add Failed"}               
    else:
        response = {'status':True,'message':'Success','data':result}
    return cloud9Lib.jsonObject(response)

def find(query):  
    print(query)
    sys.stdout.flush()
    if 'expired_otp' in query:
        query['expired_otp'] ={"$gt": query['expired_otp']}
    result = db.find(collection,query)
    if result == []:
        response = {"status":False, "data":query}               
    else:
        response = {'status':True, 'data':result}    
    return cloud9Lib.jsonObject(response)

def findOne(query):  
    if 'expired_otp' in query:
        query['expired_otp'] ={"$gt": query['expired_otp']}
    result = db.findOne(collection,query, None)
    
    if (result is None) or (result is False):
        response = {"status":False, "data":query}               
    else:
        response = {'status':True,'message':'Success','data':result}    
    return cloud9Lib.jsonObject(response)

def update(query,data):            
    updateData = {}
    if 'username' in data: updateData['username'] = data['username']
    if 'password' in data: updateData['password'] = data['password']
    if 'email' in data: updateData['email'] = data['email']
    if 'name' in data: updateData['name'] = data['name']
    if 'otp' in data: updateData['otp'] = data['otp']
    if 'expired_otp' in data: updateData['expired_otp'] = data['expired_otp']
    if 'active' in data: updateData['active'] = data['active']
    if updateData == []:
        return {"status":False, "message":"UPDATE NONE"}        
    result = db.updateData(collection,query,updateData)
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
    sys.stdout.flush()
    return cloud9Lib.jsonObject(response)

    