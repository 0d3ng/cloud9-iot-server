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
import os

sensors = []
db = db.dbmongo()
elastic = elastic.elastic()
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


def etl(collection,elastic_index,info,device_code,message):  #info --> , channel_type,topic,token_access,ip_sender,date_add_sensor
    insertQuery = info
    insertQuery['raw_message'] = message
    print("------------------")
    sys.stdout.flush()
    insertQuery['date_add_server'] = datetime.datetime.now(timezone('Asia/Tokyo')) #datetime.datetime.utcnow() #datetime.datetime.utcnow()
    insertQuery['date_add_server_unix'] = round(datetime.datetime.now(timezone('Asia/Tokyo')).timestamp() * 1000) #round(datetime.datetime.utcnow().timestamp() * 1000) #datetime.datetime.utcnow()
    insertQuery['device_code'] = device_code
    print(insertQuery['date_add_server'])
    print(insertQuery['date_add_server_unix'])

    queryDevice = {
        'device_code' : device_code
    }
    deviceData = deviceController.findOne(queryDevice)
    if deviceData['status'] == True :
        deviceData = deviceData['data']['field']

        for fieldData in deviceData:
            if type(fieldData) is dict:
                fieldName = list(fieldData.keys())[0]
            else:
                fieldName = fieldData
            insertQuery[fieldName] = extract_etl(fieldData,message,collection,device_code)

    insertElastic = copy.copy(insertQuery)
    # print(collection)
    # print(insertQuery)
    # print("------------------")
    sys.stdout.flush()
    result = db.insertData(collection,insertQuery)
    if result == []:
        response = {'status':False, 'message':"Add Failed"}               
    else:        
        response = {'status':True,'message':'Success','data':result}   
        mqttcom.publish("mqtt/output/"+elastic_index,insertElastic)    
        # elastic.insertOne(elastic_index,insertElastic) 
        del insertElastic['raw_message']
        # mqttcom.publish("message/ouput/"+elastic_index,insertElastic)    
    print(response)
    sys.stdout.flush()
    return cloud9Lib.jsonObject(response)

def extract_etl(field,data,collection,device_code):
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
                    result[item] = extract_etl(item,data[fieldName],collection,device_code)
            return result
        else:
            return None
    else:
        if field in data:
            return data[field]
        else:
            return None


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
        mqttcom.publish("mqtt/output/"+elastic_index,insertElastic)    
    return cloud9Lib.jsonObject(response)