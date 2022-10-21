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
join_operator_list = ["and","or"]

collection = "rules"
prefix_collection_sensor = "sensor_data_"
prefix_collection_schema = "schema_data_"

def add(fillData):  
    insertQuery = {
        'rules_code':fillData.get('rules_code', None),
        'name':fillData.get('name', None),
        'parameter_data':fillData.get('parameter_data', None), #json object for data
        'object_format':fillData.get('object_format', None), #json object for rules
        'script_format':fillData.get('script_format', None), #json object for rules
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

    if 'rules_code' in query: queryUpdate['rules_code'] = query['rules_code']
    if '_id' in query: queryUpdate['_id'] = query['_id']
    
    if 'name' in data: updateData['name'] = data['name']
    if 'rules_code' in data: updateData['rules_code'] = data['rules_code']
    if 'parameter_data' in data: updateData['parameter_data'] = data['parameter_data']
    if 'object_format' in data: updateData['object_format'] = data['object_format']    
    if 'script_format' in data: updateData['script_format'] = data['script_format']    
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

def converter(rules):
    rules_string=""
    rules_list = rules["rules"]
    rules_string_list={}
    for i in rules_list:
        rules_string_list["rules"+i] = str(rules_list[i][0])+" "+str(rules_list[i][1])+" "+str(rules_list[i][2])

    rules_join = rules["join"]
    for item in rules_join:
        if(item not in join_operator_list):
            rules_string+=" ("+rules_string_list[item]+") "
        else:
            rules_string+=" "+item+""
    return rules_string

def evaluation(sensor_data,rules_data,rules_string):
    params_string = ""
    for item in rules_data:
        if( item in sensor_data):
            value = sensor_data[item]
        else:
            value = 0
        params_string+= "_"+item+"_ = "+str(value)+"\n"    
    exec(params_string)
    return eval(rules_string)

def item_deconverter(script):
    script.replace("(", "").replace(")", "")
    script_item = script.split(" ")
    return script_item

def deconverter(script):
    script_join = script.split("  ")
    rules_list = {}
    join_list = [] 
    rules_number = 1
    for item in script_join:
        item = str(item).strip()
        if( item in  join_operator_list):
            join_list.append(item)
        else:
            join_list.append("rules"+str(rules_number))
            rules_list[str(rules_number)] = item_deconverter(item)
            rules_number +=1