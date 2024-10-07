#!/usr/bin/python3

import sys
from bson import ObjectId
import json 
from function import db,cloud9Lib
import datetime

sensors = []
db = db.dbmongo()

collection = "schema"
prefix_collection = "schema_"

def add(fillData):  
    insertQuery = {
        'schema_code':fillData.get('schema_code', None),
        'name':fillData.get('name', None),
        'field':fillData.get('field', None), #arraylist [field on schema] str, int, float, bool, date, time, datetime
        'date_add': datetime.datetime.utcnow(),
        'add_by':fillData.get('add_by', None),
        'active':fillData.get('active', False),
        'information':fillData.get('information', None), #array[detail, purpose]        
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
    if 'schema_code' in query: queryUpdate['schema_code'] = query['schema_code']
    if '_id' in query: queryUpdate['_id'] = query['_id']
    
    if 'name' in data: updateData['name'] = data['name']
    if 'active' in data: updateData['active'] = data['active']
    if 'schema_code' in data: updateData['schema_code'] = data['schema_code']
    if 'field' in data: updateData['field'] = data['field']
    if 'information' in data: updateData['information'] = data['information']
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
    result = db.deleteData(collection,query)
    if not result:
        response = {"status":False, "message":"DELETE FAILED"}               
    else:
        response = {'status':True,'message':'Success','data':result}
    return cloud9Lib.jsonObject(response)

