#!/usr/bin/python3

import sys
from bson import ObjectId
import json 
from function import *
import datetime
import pandas as pd
from controller import schemaDataController
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

collection = "combi"
prefix_collection = "combi_"
prefix_collection_sensor = "sensor_data_"
prefix_collection_schema = "schema_data_"

def add(fillData):  
    insertQuery = {
        'combi_code':fillData.get('combi_code', None),
        'schema_code':fillData.get('schema_code', None),
        'name':fillData.get('name', None),
        'field':fillData.get('field', None), #arraylist [field on schema] str, int, float, bool, date, time, datetime
        'date_add': datetime.datetime.utcnow(),
        'add_by':fillData.get('add_by', None),
        'stream':fillData.get('stream', False),
        'time_loop':fillData.get('time_loop', None), #in minutes
        'information':fillData.get('information', None), #array[detail, purpose]        
    }
    result = db.insertData(collection,insertQuery)
    if result == []:
        response = {'status':False, 'message':"Add Failed"}               
    else:
        response = {'status':True,'message':'Success','data':result}
        if insertQuery['stream'] == True:
            triggerService(insertQuery["combi_code"],insertQuery["time_loop"],True)
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
    if 'combi_code' in query: queryUpdate['combi_code'] = query['combi_code']
    if '_id' in query: queryUpdate['_id'] = query['_id']
    
    if 'name' in data: updateData['name'] = data['name']
    if 'active' in data: updateData['active'] = data['active']
    if 'combi_code' in data: updateData['combi_code'] = data['combi_code']
    if 'schema_code' in data: updateData['schema_code'] = data['schema_code']
    if 'field' in data: updateData['field'] = data['field']
    if 'stream' in data: updateData['stream'] = data['stream']
    if 'active' in data: updateData['active'] = data['active']
    if 'time_loop' in data: updateData['time_loop'] = data['time_loop']
    if 'updated_by' in data: updateData['updated_by'] = data['updated_by']

    if updateData == []:
        return {"status":False, "message":"UPDATE NONE"}   
    last = findOne(queryUpdate)['data'] 
    # print(updateData)
    # print(last)
    # sys.stdout.flush()    
    result = db.updateData(collection,queryUpdate,updateData)
    if not result :
        response = {"status":False, "message":"UPDATE FAILED"}               
    else:
        response = {'status':True,'message':'Success','data':result}
        if last['stream'] !=  updateData['stream']:
            if 'time_loop' in updateData: 
                time_loop =  updateData['time_loop']
            else:
                time_loop = last['time_loop']
            if updateData['stream'] == True:
                triggerService(last["combi_code"],time_loop,True)
            if updateData['stream'] == False:
                triggerService(last["combi_code"],time_loop,False)
    return cloud9Lib.jsonObject(response)

def delete(query): 
    listData = findOne(query)['data']           
    result = db.deleteData(collection,query)
    if not result:
        response = {"status":False, "message":"DELETE FAILED"}               
    else:
        response = {'status':True,'message':'Success','data':result}
        triggerService(listData["combi_code"],listData["time_loop"],False)
    return cloud9Lib.jsonObject(response)

def triggerService(combi_code,time_loop,status):
    send = {
        "combi_code":combi_code,
        "time_loop":time_loop
    }
    if status :
        mqttcom.publish(config["MQTT"]["combi_stream_start"],send)
        return
    else:
        mqttcom.publish(config["MQTT"]["combi_stream_stop"],send)
        return

def getSensorData(time_str,time_end,code,key,value,collectid=None):
    if(collectid):
        collection = prefix_collection_sensor+str(collectid)
    else:
        collection = prefix_collection_sensor+str(code)
    datesrc_str = datetime.datetime.strptime(time_str+":00",'%Y-%m-%d %H:%M:%S') - td
    datesrc_end = datetime.datetime.strptime(time_end+":59",'%Y-%m-%d %H:%M:%S') - td
    query = {
        'date_add_server' : {"$gte":datesrc_str, "$lt":datesrc_end } ,
        str(key):{"$ne":None,"$ne":""},
        str(value):{"$ne":None}
    }
    exclude = {
        str(key):1,
        str(value):1,
        "date_add_server":1
    }
    if(collectid):
        query["device_code"] = str(code)
    # print("--------------------")
    # print(collection)
    # print(query)
    
    result = sensorController.find(collection,query,exclude)
    # print(result)
    # print("-------++++---------")
    sys.stdout.flush()
    if not result['status']:
        response = []               
    else:
        response = []
        for item in result['data']:
            response.append([item[str(key)],item[str(value)]])
    return response

def grouping(datalist,method):
    d = {}
    for key, val in datalist:
        if method == "average" or method == "variance" :
            if not cloud9Lib.is_float(val) :
                continue
        if not key in d:
            d[key] = []
        d[key].append(val)
    return d 

def averagedata(datalist):
    for key in datalist:
        # datalist[key] = sum(datalist[key]) / len(datalist[key])
        try:
            datalist[key] = statistics.mean(datalist[key])        
        except:
            print(datalist)
            print("ERROR")
            datalist[key] = 0
            sys.stdout.flush()
    return datalist

def variancedata(datalist):
    for key in datalist:
        try:
            datalist[key] = statistics.variance(datalist[key])        
        except:
            print(datalist)
            print("ERROR")
            datalist[key] = 0
            sys.stdout.flush()
    return datalist

def generateDate(time_str,time_end,freq):
    time_str = datetime.datetime.strptime(time_str,'%Y-%m-%d %H:%M')
    time_end = datetime.datetime.strptime(time_end,'%Y-%m-%d %H:%M')
    time_str = time_str.strftime('%Y-%m-%dT%H:%M:00Z')
    time_end = time_end.strftime('%Y-%m-%dT%H:%M:00Z')
    freq = str(freq)+"T"
    l = (pd.DataFrame(columns=['NULL'],
                    index=pd.date_range(time_str, time_end,freq=freq))
        .index.strftime('%Y-%m-%d %H:%M')
        .tolist()
    )
    return l

def combiProcess(schema_code,field,time_start,time_end,batch_code = None,send_result = None):
    insertCount = 0
    insertData = {}
    for fieldData in field:
        fieldName = list(fieldData.keys())[0]
        fieldValue = fieldData[fieldName]
        if(str(fieldValue) != "key"):
            code = fieldValue["data"][0]
            field_val_search = fieldValue["data"][1]
            field_key_search = fieldValue["data"][2]
            method = fieldValue["option"]
            if "collectid" in fieldValue:
                datalist = getSensorData(time_start,time_end,code,field_key_search,field_val_search,fieldValue["collectid"])
            else :
                datalist = getSensorData(time_start,time_end,code,field_key_search,field_val_search)
            datalist = grouping(datalist,method)
            if(method == "average"):            
                datalist = averagedata(datalist)
            if(method == "variance"):
                datalist = variancedata(datalist)            
            for key in datalist:
                if not key in insertData:
                    insertData[key] = {}
                insertData[key].update({fieldName:datalist[key]})
    for itemsKey in insertData:
        insertQuery = {}
        itemData = insertData[itemsKey]
        for fieldData in field:
            fieldName = list(fieldData.keys())[0]
            fieldValue = fieldData[fieldName]
            if(str(fieldValue) == "key"):
                insertQuery[fieldName] = itemsKey
            else:
                if(not fieldName in itemData):
                    insertQuery[fieldName] = fieldValue["default"]
                else:
                    insertQuery[fieldName] = itemData[fieldName]
        filter = schemaDataController.filter(schema_code,insertQuery)
        if filter['status']:
            insertQuery = filter['data']
            if batch_code is not None :
                insertQuery["batch_code"] = batch_code
            if send_result is None:
                insertQuery["date_add_batch"] = datetime.datetime.now(timezone('Asia/Tokyo'))
            insertQuery["date_add_auto"] = datetime.datetime.strptime(time_end,'%Y-%m-%d %H:%M') - td            
            try:
                insert = schemaDataController.add(prefix_collection_schema+schema_code,insertQuery)
                if insert['status']: 
                    insertCount = insertCount + 1
            except:
                print(schema_code)
                print(insertQuery)
                print("ERROR")
                sys.stdout.flush()

    return insertCount