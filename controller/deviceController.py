#!/usr/bin/python3

import sys
from bson import ObjectId
import json 
from function import *
import datetime
from controller import comChannelController

sensors = []
db = db.dbmongo()

collection = "device"
prefix_collection = "sensor_data_"
prefix_elastic = "device-"

def add(fillData):  
    insertQuery = {
        'group_code_name':fillData.get('group_code_name', None),
        'key_access':fillData.get('key_access', None),
        'device_code':fillData.get('device_code', None),
        'name':fillData.get('name', None),
        'field':fillData.get('field', None), #arraylist [field on sensor]
        'date_add': datetime.datetime.utcnow(),
        'add_by':fillData.get('add_by', None),
        'active':fillData.get('active', False),
        'information':fillData.get('information', None), #array[location, detail, purpose]        
    }
    result = db.insertData(collection,insertQuery)
    if result == []:
        response = {'status':False, 'message':"Add Failed"}               
    else:
        response = {'status':True,'message':'Success','data':result}
    return cloud9Lib.jsonObject(response)

def find(query):  
    result = db.find(collection,query)
    print(result)
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
    if 'code_name' in query: queryUpdate['code_name'] = query['code_name']
    if '_id' in query: queryUpdate['_id'] = query['_id']
    
    if 'name' in data: updateData['name'] = data['name']
    if 'active' in data: updateData['active'] = data['active']
    if 'key_access' in data: updateData['key_access'] = data['key_access']
    if 'device_code' in data: updateData['device_code'] = data['device_code']
    if 'field' in data: updateData['field'] = data['field']
    if 'information' in data: updateData['information'] = data['information']
    if 'updated_by' in data: updateData['updated_by'] = data['updated_by']
    if 'token_access' in data: updateData['token_access'] = data['token_access']
    if 'field_process' in data: updateData['field_process'] = data['field_process']

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

def addOther(fillData):  
    insertQuery = {
        'group_code_name':fillData.get('group_code_name', None),
        'key_access':fillData.get('key_access', None),
        'device_code':fillData.get('device_code', None),
        'name':fillData.get('name', None),
        'field':fillData.get('field', None), #arraylist [field on sensor]
        'date_add': datetime.datetime.utcnow(),
        'add_by':fillData.get('add_by', None),
        'active':fillData.get('active', False),
        'information':fillData.get('information', None), #array[location, detail, purpose]        
        'communication':fillData.get('communication', None), #array[location, detail, purpose]        
    }
    result = db.insertData(collection,insertQuery)
    if result == []:
        response = {'status':False, 'message':"Add Failed"}               
    else:
        response = {'status':True,'message':'Success','data':result}
        if 'communication' in fillData :
            insertComm = fillData['communication']
            insertComm['device_code'] = insertQuery['device_code']
            insertComm['token_access'] = insertQuery['key_access']
            insertComm['index_log'] = prefix_elastic + fillData['device_code']
            if 'topic' not in insertComm :
                insertComm['topic'] = insertQuery['device_code']
            communication_add(insertComm)

    return cloud9Lib.jsonObject(response)

def updateOther(query,data):            
    updateData = {}
    queryUpdate = {}
    if 'code_name' in query: queryUpdate['code_name'] = query['code_name']
    if '_id' in query: queryUpdate['_id'] = query['_id']
    
    if 'name' in data: updateData['name'] = data['name']
    if 'active' in data: updateData['active'] = data['active']
    if 'key_access' in data: updateData['key_access'] = data['key_access']
    if 'device_code' in data: updateData['device_code'] = data['device_code']
    if 'field' in data: updateData['field'] = data['field']
    if 'information' in data: updateData['information'] = data['information']
    if 'updated_by' in data: updateData['updated_by'] = data['updated_by']
    if 'communication' in data: updateData['communication'] = data['communication']

    if updateData == []:
        return {"status":False, "message":"UPDATE NONE"}        
    result = db.updateData(collection,queryUpdate,updateData)
    if not result :
        response = {"status":False, "message":"UPDATE FAILED"}               
    else:
        response = {'status':True,'message':'Success','data':result}
        if 'communication' in updateData :
            if 'key_access' in data:
                token = data['key_access']
            else:
                token = findOne(queryUpdate)['data']['key_access']
            updateComm = updateData['communication']
            updateComm['token_access'] = token
            communication_update(updateComm)

    return cloud9Lib.jsonObject(response)

def deleteOther(query,itemData):            
    result = db.deleteData(collection,query)
    if not result:
        response = {"status":False, "message":"DELETE FAILED"}               
    else:
        response = {'status':True,'message':'Success','data':result}
        deleteComm = {
            'device_code':itemData['device_code']
        }
        comChannelController.deleteOther(deleteComm)
    return cloud9Lib.jsonObject(response)

def communication_add(fillData):
    insertcomm = {
        'token_access':fillData['token_access'],
        'device_code':fillData['device_code'],
        'collection_name':prefix_collection+fillData['device_code'],
        'index_log':fillData['index_log']
    }
    
    if 'http-post' in fillData :
        insertHttp = insertcomm
        insertHttp['channel_code'] = 'http-post-'+fillData['token_access']
        insertHttp['channel_type'] = 'http-post'
        insertHttp['active'] = fillData['http-post']
        comChannelController.addOther(insertHttp)

    if 'mqtt' in fillData :
        insertMqtt = insertcomm
        insertMqtt['channel_code'] = 'mqtt-'+fillData['token_access']
        insertMqtt['channel_type'] = 'mqtt'
        insertMqtt['server'] = fillData['server']
        insertMqtt['port'] = fillData['port']
        insertMqtt['topic'] = fillData['topic']
        insertMqtt['active'] = fillData['mqtt']
        comChannelController.addOther(insertMqtt)

    # if 'nats' in fillData :
    #     insertNats = insertcomm;
    #     insertNats['channel_code'] = 'nats-'+fillData['token_access']
    #     insertNats['channel_type'] = 'nats'
    #     insertNats['topic'] = prefix_topic+fillData['topic']
    #     insertNats['active'] = fillData['nats']
    #     comChannelController.add(insertNats)

    # if 'kafka' in fillData :
    #     insertKafka = insertcomm;
    #     insertKafka['channel_code'] = 'kafka-'+fillData['token_access']
    #     insertKafka['channel_type'] = 'kafka'
    #     insertKafka['topic'] = prefix_topic2+fillData['topic']
    #     insertKafka['active'] = fillData['kafka']
    #     comChannelController.add(insertKafka)

def communication_update(fillData):
    if 'http-post' in fillData :
        query = {
            'channel_code': 'http-post-'+fillData['token_access']
        }
        commdata = comChannelController.findOne(query)
        if commdata['status']:
            commdata = commdata['data']
            if commdata['active'] != fillData['http-post'] : 
                updateHttp = query
                updateHttp['active'] = fillData['http-post']
                updateHttp['channel_type'] = 'http-post'
                comChannelController.updateOther(query,updateHttp)

    if 'mqtt' in fillData :
        query = {
            'channel_code': 'mqtt-'+fillData['token_access']
        }
        commdata = comChannelController.findOne(query)
        if commdata['status']:
            commdata = commdata['data']
            if ( commdata['active'] != fillData['mqtt']) or ( commdata['server'] != fillData['server']) or ( commdata['port'] != fillData['port']) or ( commdata['topic'] != fillData['topic'])  : 
                updateMqtt = query
                updateMqtt['active'] = fillData['mqtt']
                updateMqtt['channel_type'] = 'mqtt'
                updateMqtt['server'] = fillData['server']
                updateMqtt['port'] = fillData['port']
                updateMqtt['topic'] = fillData['topic']
                comChannelController.updateOther(query,updateMqtt)

    # if 'nats' in fillData :
    #     query = {
    #         'channel_code': 'nats-'+fillData['token_access']
    #     }
    #     commdata = comChannelController.findOne(query)['data']
    #     if commdata :
    #         if commdata['active'] != fillData['nats'] : 
    #             updateNats = query
    #             updateNats['active'] = fillData['nats']
    #             updateNats['channel_type'] = 'nats'
    #             updateNats['topic'] = commdata['topic']
    #             comChannelController.update(query,updateNats)

    # if 'kafka' in fillData :
    #     query = {
    #         'channel_code': 'kafka-'+fillData['token_access']
    #     }
    #     commdata = comChannelController.findOne(query)['data']
    #     if commdata :
    #         if commdata['active'] != fillData['kafka'] : 
    #             updateKafka = query
    #             updateKafka['active'] = fillData['kafka']
    #             updateKafka['channel_type'] = 'kafka'
    #             updateKafka['topic'] = commdata['topic']
    #             comChannelController.update(query,updateKafka)

def updateSensorData(collection,queryUpdate,updateData):            
    if updateData == []:
        return {"status":False, "message":"UPDATE NONE"}        
    result = db.updateData(collection,queryUpdate,updateData)
    if not result :
        response = {"status":False, "message":"UPDATE FAILED"}               
    else:
        response = {'status':True,'message':'Success','data':result}
    return cloud9Lib.jsonObject(response)

#==============================================================