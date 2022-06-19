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

collection = "datasync"
prefix_collection = "datasync_"
prefix_collection_sensor = "sensor_data_"
prefix_collection_schema = "schema_data_"

def add(fillData):  
    insertQuery = {
        'datasync_code':fillData.get('datasync_code', None),
        'schema_code':fillData.get('schema_code', None),
        'name':fillData.get('name', None),
        'field':fillData.get('field', None), #arraylist [field on schema] str, int, float, bool, date, time, datetime
        'date_add': datetime.datetime.utcnow(),
        'add_by':fillData.get('add_by', None),
        'stream':fillData.get('stream', False),
        'time_loop':fillData.get('time_loop', None), #in second
        'information':fillData.get('information', None), #array[detail, purpose]        
    }
    result = db.insertData(collection,insertQuery)
    if result == []:
        response = {'status':False, 'message':"Add Failed"}               
    else:
        response = {'status':True,'message':'Success','data':result}
        if insertQuery['stream'] == True:
            triggerService(insertQuery["datasync_code"],insertQuery["time_loop"],True)
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
    if 'datasync_code' in query: queryUpdate['datasync_code'] = query['datasync_code']
    if '_id' in query: queryUpdate['_id'] = query['_id']
    
    if 'name' in data: updateData['name'] = data['name']
    if 'active' in data: updateData['active'] = data['active']
    if 'datasync_code' in data: updateData['datasync_code'] = data['datasync_code']
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
                triggerService(last["datasync_code"],time_loop,True)
            if updateData['stream'] == False:
                triggerService(last["datasync_code"],time_loop,False)
    return cloud9Lib.jsonObject(response)

def delete(query): 
    listData = findOne(query)['data']           
    result = db.deleteData(collection,query)
    if not result:
        response = {"status":False, "message":"DELETE FAILED"}               
    else:
        response = {'status':True,'message':'Success','data':result}
        triggerService(listData["datasync_code"],listData["time_loop"],False)
    return cloud9Lib.jsonObject(response)

def triggerService(datasync_code,time_loop,status):
    send = {
        "datasync_code":datasync_code,
        "time_loop":time_loop
    }
    print("----------------------")
    print(send)
    if status :
        print(config["MQTT"]["datasync_stream_start"])
        mqttcom.publish(config["MQTT"]["datasync_stream_start"],send)
        return
    else:
        print(config["MQTT"]["datasync_stream_stop"])
        mqttcom.publish(config["MQTT"]["datasync_stream_stop"],send)
        return


def getSensorData(time_str,time_end,code,key,value,collectid=None):
    if(collectid):
        collection = prefix_collection_sensor+str(collectid)
    else:
        collection = prefix_collection_sensor+str(code)
    datesrc_str = datetime.datetime.strptime(time_str,'%Y-%m-%d %H:%M:%S') - td
    datesrc_end = datetime.datetime.strptime(time_end,'%Y-%m-%d %H:%M:%S') - td
    query = {
        'date_add_server': {"$gte": datesrc_str,"$lt": datesrc_end},
        "$and":[{str(key):{"$ne":None}},{str(key):{"$ne":""}}]
    }    
    if(collectid):
        query["device_code"] = str(code)        
    
    group = {
        "_id":"$"+str(key)                
    }
    for item in value:
        group[str(item)] = {"$push":"$"+str(item)}

    pipeline = [{"$unwind":"$"+str(key) },
            {"$match":query},
            {"$sort" : { 'date_add_server' : -1 }},
            {"$group":group}]
    # print("--------------------")
    # print(collection)
    # print(pipeline)    
    result = db.aggregate(collection,pipeline)
    response = {}
    itemkey = []
    for item in result:
        response[item[str(key)]] = item
        itemkey.append(item[str(key)])
    # print(response)
    # print("-------++++---------")
    sys.stdout.flush()
    return response,itemkey

def averagedata(datalist,defaultval):
    try:
        df = pd.DataFrame(datalist)
        datalist = df[df[0].apply(lambda x: is_float(x))]
        return statistics.mean(datalist[0])
    except:
        print(datalist)
        print("ERROR Average")
        return defaultval

def variancedata(datalist,defaultval):
    try:
        df = pd.DataFrame(datalist)
        datalist = df[df[0].apply(lambda x: is_float(x))]
        return statistics.variance(datalist[0])
    except:
        return defaultval

def maxdata(datalist,defaultval):
    try:
        df = pd.DataFrame(datalist)
        datalist = df[df[0].apply(lambda x: is_float(x))]
        return max(datalist[0])
    except:
        print(datalist)
        print("ERROR Max")
        return defaultval

def mindata(datalist,defaultval):
    try:
        df = pd.DataFrame(datalist)
        datalist = df[df[0].apply(lambda x: is_float(x))]
        return min(datalist[0])
    except:
        print(datalist)
        print("ERROR Min")
        return defaultval

def currentdata(datalist,defaultval):
    try:
        return datalist[len(datalist)]
    except:
        return defaultval

# def firstdata(datalist,defaultval):
#     try:
#         return datalist[0]
#     except:
#         return defaultval

# def lastdata(datalist,defaultval):
#     try:
#         return datalist[len(datalist)]
#     except:
#         return defaultval

def is_float(x):
    try:
        float(x)
    except ValueError:
        return False
    return True

def generateDate(time_str,time_end,freq):
    time_str = datetime.datetime.strptime(time_str,'%Y-%m-%d %H:%M:%S')
    time_end = datetime.datetime.strptime(time_end,'%Y-%m-%d %H:%M:%S')
    time_str = time_str.strftime('%Y-%m-%dT%H:%M:%SZ')
    time_end = time_end.strftime('%Y-%m-%dT%H:%M:%SZ')
    freq = str(freq)+"S"
    l = (pd.DataFrame(columns=['NULL'],
                    index=pd.date_range(time_str, time_end,freq=freq))
        .index.strftime('%Y-%m-%d %H:%M:%S')
        .tolist()
    )
    return l

def datasyncProcess(schema_code,field,time_start,time_end,batch_code = None,send_result = None):
    insertCount = 0
    dataSource = {}
    listKey = []
    # print("start")
    for fieldData in field:
        fieldName = list(fieldData.keys())[0]
        fieldValue = fieldData[fieldName]
        if(str(fieldValue) != "key"):
            code = fieldValue["data"][0]
            field_val_search = fieldValue["data"][1]
            field_key_search = fieldValue["data"][2]
            if(code not in dataSource):
                dataSource[code] = {}
            if(field_key_search not in dataSource[code]):
                dataSource[code][field_key_search] = []
            if(field_val_search not in dataSource[code][field_key_search]):
                dataSource[code][field_key_search].append(field_val_search)            
    for code in dataSource:
        for key_search in dataSource[code]:
            dataSource[code][key_search],itemKey = getSensorData(time_start,time_end,code,key_search,dataSource[code][key_search])
            listKey= listKey + list(set(itemKey)-set(listKey))

    dataSource = pd.DataFrame(dataSource)
    dataSource = dataSource.dropna()
    for itemKey in listKey:
        insertQuery = {}
        for fieldData in field:
            fieldName = list(fieldData.keys())[0]
            fieldValue = fieldData[fieldName]
            if(str(fieldValue) == "key"):
                insertQuery[fieldName] = itemKey
            else:
                code = fieldValue["data"][0]
                field_val_search = fieldValue["data"][1]
                field_key_search = fieldValue["data"][2]
                method = fieldValue["option"]
                if itemKey in dataSource[code][field_key_search]:
                    item_ds = dataSource[code][field_key_search][itemKey][field_val_search]
                    if method == 'average':
                        insertQuery[fieldName] = averagedata(item_ds,fieldValue["default"])
                    elif method == 'variance':
                        insertQuery[fieldName] = variancedata(item_ds,fieldValue["default"])
                    elif method == 'current':
                        insertQuery[fieldName] = currentdata(item_ds,fieldValue["default"])
                    elif method == 'max':
                        insertQuery[fieldName] = maxdata(item_ds,fieldValue["default"])
                    elif method == 'min':
                        insertQuery[fieldName] = mindata(item_ds,fieldValue["default"])
                    else:
                        insertQuery[fieldName] = fieldValue["default"]
                else:
                    insertQuery[fieldName] = fieldValue["default"]

        filter = schemaDataController.filter(schema_code,insertQuery)
        if filter['status']:
            insertQuery = filter['data']
            # if batch_code is not None :
            #     insertQuery["batch_code"] = batch_code
            # if send_result is None:
            #     insertQuery["date_add_batch"] = datetime.datetime.now(timezone('Asia/Tokyo'))
            insertQuery["date_add_auto"] = datetime.datetime.strptime(time_end,'%Y-%m-%d %H:%M:%S') - td            
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