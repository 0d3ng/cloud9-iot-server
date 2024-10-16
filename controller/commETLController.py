#!/usr/bin/python3

import sys
from bson import ObjectId
import json
from function import *
import datetime
from controller import deviceController
from pytz import timezone
import copy
import base64
import time
import os


sensors = []
db = db.dbmongo()
# elastic = elastic.elastic()
main_folder = "data"

def add_dir(new_dir):
    if not os.path.exists(new_dir):
        os.makedirs(new_dir)

def save_image(message,field,group,device_name):
    try:
        folder = main_folder + "/" + group
        imagesFile = base64.b64decode(message)
        file_name = device_name + "_" + field + "_" + str(round(datetime.datetime.now(timezone('Asia/Tokyo')).timestamp() * 1000))
        add_dir(folder)
        image = open(folder+"/"+file_name+".png", "wb+")
        image.write(imagesFile)
        image.close()
        res = {
            'type':'image',
            'url': file_name+".png"
        }
        print("Save Image")
    except Exception as e:
        print("Failed Save Image")
        print(e)
    return res


def etl(collection,elastic_index,info,device_code,message,receive_time = None):  #info --> , channel_type,topic,token_access,ip_sender,date_add_sensor
    insertQuery = info
    insertQuery['raw_message'] = copy.copy(message)
    insertElastic = copy.copy(insertQuery)
    insertQuery['date_add_server'] = datetime.datetime.now(timezone('Asia/Tokyo')) #datetime.datetime.utcnow() #datetime.datetime.utcnow()
    #round(datetime.datetime.now(timezone('Asia/Tokyo')).timestamp() * 1000) #round(datetime.datetime.utcnow().timestamp() * 1000) #datetime.datetime.utcnow()
    insertQuery['device_code'] = device_code
    # print("------------------")
    # sys.stdout.flush()    
    # print(insertQuery['date_add_server'])
    # print(insertQuery['date_add_server_unix'])

    queryDevice = {
        'device_code' : device_code
    }
    deviceData = deviceController.findOne(queryDevice)
    state = False
    message = keys_lower(message)
    if deviceData['status'] == True :
        deviceProcess = False
        if 'field_process' in deviceData['data']:
            deviceProcess = deviceData['data']['field_process']
        deviceData = deviceData['data']['field']
        for fieldData in deviceData:
            if type(fieldData) is dict:
                fieldName = list(fieldData.keys())[0]
            else:
                fieldName = fieldData
            insertQuery[fieldName],state = extract_etl(fieldData,message,collection,device_code,state)
        
        if state and deviceProcess:
            for fieldkey in deviceProcess:
                fielditem = deviceProcess[fieldkey]
                insertQuery[fieldkey] = preproces(insertQuery,fielditem)

    # print("------------------")
    # print(collection)
    # print(insertQuery)
    # print("------------------")
    # sys.stdout.flush()
    # result = db.insertData(collection,insertQuery)
    ### CODINGHACK ####
    # hack = False
    # if 'ts' in insertQuery and 'id' in insertQuery:
    #     if insertQuery['id'] == None:
    #         hack = True
    # if hack:
    if state == False:
        result = []
    else:
        insertQuery['receive_unix_time'] = receive_time
        insertQuery['save_unix_time'] = round(datetime.datetime.now(datetime.timezone.utc).timestamp()*1000)
        result = db.insertData(collection,insertQuery)  
    ### END CODINGHACK #### 
    if result == []:
        response = {'status':False, 'message':"Add Failed"}               
    else:        
        response = {'status':True,'message':'Success','data':result}   
        del insertQuery['raw_message']
        insertQuery["date_add_server"] = round(insertQuery["date_add_server"].timestamp()*1000)
        insertQuery["_id"] = str(insertQuery["_id"])
        # Tutup sementara
        mqttcom.publish("mqtt/output/"+elastic_index,insertQuery)    
        
    return cloud9Lib.jsonObject(response)

def extract_etl(field,data,collection,device_code,state=False):
    if type(field) is dict:
        fieldName = list(field.keys())[0]      
        if fieldName in data:
            result = {}
            if field[fieldName] == 'image' :
                result = save_image(data[fieldName],fieldName,collection,device_code)
            else :
                for item in field[fieldName]:
                    if type(item) is dict:
                        itemName = list(item.keys())[0]
                    else:
                        itemName = item                
                    result[itemName],state = extract_etl(item,data[fieldName],collection,device_code,state)
            return result,state
        else:
            return None,state
    else:
        if field in data: ##This Code should change to lowercase
            if state == False :
                if len(str(data[field])) > 0:
                    state = True
            return data[field],state
        else:
            return None,state


def nonetl(collection,elastic_index,info,message):  #info --> device_code, channel_type,topic,token_access,ip_sender,date_add_sensor
    insertQuery = info
    insertQuery['raw_message'] = message
    insertQuery['date_add_server'] = datetime.datetime.today() #datetime.datetime.utcnow()
    insertElastic = copy.copy(insertQuery)
    result = db.insertData(collection,insertQuery)
    if result == []:
        response = {'status':False, 'message':"Add Failed"}               
    else:        
        response = {'status':True,'message':'Success','data':result} 
        # elastic.insertOne(elastic_index,insertElastic)
        insertQuery["date_add_server"] = round(insertQuery["date_add_server"].timestamp()*1000)
        insertQuery["_id"] = str(insertQuery["_id"])
        mqttcom.publish("mqtt/output/"+elastic_index,insertElastic)    
    return cloud9Lib.jsonObject(response)


def preproces(insert,data):
    try: 
        exec(data['pre'])
        var = []
        for x in range(len(data['var'])):
            if str(data['var'][x]) in insert:
                var.append(insert[str(data['var'][x])])
            else:
                var.append(0)
        return eval(data['process'])
    except:
        return 0 

def etl_inner(collection,elastic_index,deviceData,device_code,message,receive_time = None):  #info --> , channel_type,topic,token_access,ip_sender,date_add_sensor
    insertQuery = {}
    insertQuery['raw_message'] = copy.copy(message)
    insertElastic = copy.copy(insertQuery)
    insertQuery['date_add_server'] = datetime.datetime.now(timezone('Asia/Tokyo')) #datetime.datetime.utcnow() #datetime.datetime.utcnow()
    insertQuery['device_code'] = device_code
    deviceData = deviceData['field']
    state = True
    message = keys_lower(message)
    for fieldData in deviceData:
        if type(fieldData) is dict:
            fieldName = list(fieldData.keys())[0]
        else:
            fieldName = fieldData
        insertQuery[fieldName],state = extract_etl(fieldData,message,collection,device_code,state)
    
    insertQuery['save_unix_time'] = round(datetime.datetime.now(datetime.timezone.utc).timestamp()*1000)
    result = db.insertData(collection,insertQuery)  
    if result == []:
        response = {'status':False, 'message':"Add Failed"}               
    else:        
        del insertQuery['raw_message']
        insertQuery["date_add_server"] = round(insertQuery["date_add_server"].timestamp()*1000)
        insertQuery["_id"] = str(result)
        # Tutup sementara
        # mqttcom.publish("mqtt/output/"+elastic_index,insertQuery)   
        response = {'status':True,'message':'Success','data':insertQuery} 
        
    return cloud9Lib.jsonObject(response)

def keys_lower(test_dict):
    res = dict()
    for key in test_dict.keys():
        if type(test_dict[key]) is dict:
            res[key.lower()] = keys_lower(test_dict[key])
        else:
            res[key.lower()] = test_dict[key]
    return res