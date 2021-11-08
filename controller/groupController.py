#!/usr/bin/python3

import sys
from bson import ObjectId
import json 
from function import *



sensors = []
db = db.dbmongo()

collection = "group"

def add(fillData):  
    insertQuery = {
        'company_id':fillData.get('company_id', None),
        'name':fillData.get('name', None),
        'group_code':fillData.get('group_code', None),
        'email':fillData.get('email', None),
        'add_by':fillData.get('add_by', None),
        'active':True 
    }
    if 'add_by' in fillData:
        insertQuery['member'] = [
            {
                "user_id":fillData['add_by'],
                "active":True,
                "role":"owner"
            }
        ]

    result = db.insertData(collection,insertQuery)
    if result == []:
        response = {'status':False, 'message':"Add Failed"}               
    else:
        response = {'status':True,'message':'Success','data':result}
    return cloud9Lib.jsonObject(response)

def find(query):  
    result = db.find(collection,query)
    if result == []:
        response = {"status":False, "data":data}               
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
    if 'company_id' in data: updateData['company_id'] = data['company_id']
    if 'name' in data: updateData['name'] = data['name']
    if 'email' in data: updateData['email'] = data['email']
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
    return cloud9Lib.jsonObject(response)

def addMember(query,data):            
    addData = {}
    addData['role'] = 'member'
    addData['active'] = False 
    if 'user_id' in data: addData['user_id'] = data['user_id']
    if 'role' in data: addData['role'] = data['role']
    if 'active' in data: addData['active'] = data['active']
    updateData = {
        "member":addData
    }
    result = db.updatePush(collection,query,updateData)
    if not result :
        response = {"status":False, "message":"UPDATE FAILED"}               
    else:
        response = {'status':True,'message':'Success','data':result}
    return cloud9Lib.jsonObject(response)

def removeMember(query,data):            
    addData = {}
    if 'user_id' in data: addData['user_id'] = data['user_id']
    if 'role' in data: addData['role'] = data['role']
    if 'active' in data: addData['active'] = data['active']

    if addData == []:
        return {"status":False, "message":"UPDATE NONE"}   

    updateData = {
        "member":addData
    }
    result = db.updatePull(collection,query,updateData)
    if not result :
        response = {"status":False, "message":"UPDATE FAILED"}               
    else:
        response = {'status':True,'message':'Success','data':result}
    return cloud9Lib.jsonObject(response)

def updateMember(query,oldData,data):            
    addData = {}
    addData['role'] = 'member'
    addData['active'] = False
    if 'user_id' in data: addData['user_id'] = data['user_id']
    if 'role' in data: addData['role'] = data['role']    
    if 'active' in data: addData['active'] = data['active']

    if addData == []:
        return {"status":False, "message":"UPDATE NONE"}   

    oldData = {
        "member":oldData
    }
    updateData = {
        "member":addData
    }
    result = db.updatePull(collection,query,oldData)
    if not result :
        print(oldData)
        sys.stdout.flush()
        response = {"status":False, "message":"UPDATE FAILED"}               
    else:
        result = db.updatePush(collection,query,updateData)
        if not result :
            response = {"status":False, "message":"UPDATE FAILED"}               
        else:
            response = {'status':True,'message':'Success','data':result}
    return cloud9Lib.jsonObject(response)

def getItemMember(query,user_id):            
    query['member'] = { '$elemMatch': {"user_id":user_id } };
    result = db.findOne(collection,query)
    if result is None or result is False:
        response = {"status":False, "data":result}               
    else:
        response = {'status':True, 'data':result}    
    return cloud9Lib.jsonObject(response)