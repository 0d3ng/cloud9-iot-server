#!/usr/bin/python3

import sys
from bson import ObjectId
import json 
from function import *
import datetime
from controller import comChannelController


sensors = []
db = db.dbmongo()
elastic = elastic.elastic()
prefix_topic = "message/sensor/"
prefix_topic2 = "message-sensor-"
prefix_collection = "sensor_data_"
prefix_elastic = "group-"
collection = "group_sensor"

def add(fillData):  
    insertQuery = {
        'code_name':fillData.get('code_name', None),
        'name':fillData.get('name', None),
        'date_add': datetime.datetime.utcnow(),
        'add_by':fillData.get('add_by', None),
        'active':fillData.get('active', False),
        'access_group':fillData.get('active', None), #array id group
        'information':fillData.get('information', None), #array[location, detail, purpose]
        'token_access':fillData.get('token_access', None),
        'communication':fillData.get('communication', None), #array[topic, mqtt-active, http-post-active, nats-activ/e]
        'group_type':fillData.get('group_type', None),
        'group_code':fillData.get('group_code', None),
        'view_dashboard':fillData.get('view_dashboard', None)
    }
    result = db.insertData(collection,insertQuery)
    if result == []:
        response = {'status':False, 'message':"Add Failed"}               
    else:        
        response = {'status':True,'message':'Success','data':result}
        # elastic.createIndex(fillData.get('code_name', None))
        if 'communication' in fillData :
            insertComm = fillData['communication']
            insertComm['group_id'] = cloud9Lib.jsonObject(result)
            insertComm['token_access'] = fillData['token_access']
            insertComm['index_log'] = prefix_elastic + fillData['code_name']
            if 'topic' not in insertComm :
                insertComm['topic'] = fillData['code_name']
            communication_add(insertComm)

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
    if 'code_name' in query: queryUpdate['code_name'] = query['code_name']
    if '_id' in query: queryUpdate['_id'] = query['_id']
    
    if 'name' in data: updateData['name'] = data['name']
    if 'add_by' in data: updateData['add_by'] = data['add_by']
    if 'updated_by' in data: updateData['updated_by'] = data['updated_by']
    if 'active' in data: updateData['active'] = data['active']
    if 'access_group' in data: updateData['access_group'] = data['access_group']
    if 'information' in data: updateData['information'] = data['information']
    if 'token_access' in data: updateData['token_access'] = data['token_access']
    if 'communication' in data: updateData['communication'] = data['communication']
    if 'view_dashboard' in data: updateData['view_dashboard'] = data['view_dashboard']

    if updateData == []:
        return {"status":False, "message":"UPDATE NONE"}        
    result = db.updateData(collection,queryUpdate,updateData)
    if not result :
        response = {"status":False, "message":"UPDATE FAILED"}               
    else:
        response = {'status':True,'message':'Success','data':result}
        if 'communication' in updateData :
            if 'token_access' in data:
                token = data['token_access']
            else:
                token = findOne(queryUpdate)['data']['token_access']
            updateComm = updateData['communication']
            updateComm['token_access'] = token
            communication_update(updateComm)

    return cloud9Lib.jsonObject(response)

def delete(query):            
    result = db.deleteData(collection,query)
    if not result:
        response = {"status":False, "message":"DELETE FAILED"}               
    else:
        response = {'status':True,'message':'Success','data':result}
    return cloud9Lib.jsonObject(response)

def communication_add(fillData):
    insertcomm = {
        'token_access':fillData['token_access'],
        'collection_name':prefix_collection+fillData['group_id'],
        'index_log':fillData['index_log']
    }

    if 'http-post' in fillData :
        insertHttp = insertcomm;
        insertHttp['channel_code'] = 'http-post-'+fillData['token_access']
        insertHttp['channel_type'] = 'http-post'
        insertHttp['active'] = fillData['http-post']
        comChannelController.add(insertHttp)

    if 'mqtt' in fillData :
        insertMqtt = insertcomm;
        insertMqtt['channel_code'] = 'mqtt-'+fillData['token_access']
        insertMqtt['channel_type'] = 'mqtt'
        insertMqtt['topic'] = prefix_topic+fillData['topic']
        insertMqtt['active'] = fillData['mqtt']
        comChannelController.add(insertMqtt)

    if 'nats' in fillData :
        insertNats = insertcomm;
        insertNats['channel_code'] = 'nats-'+fillData['token_access']
        insertNats['channel_type'] = 'nats'
        insertNats['topic'] = prefix_topic+fillData['topic']
        insertNats['active'] = fillData['nats']
        comChannelController.add(insertNats)

    if 'kafka' in fillData :
        insertKafka = insertcomm;
        insertKafka['channel_code'] = 'kafka-'+fillData['token_access']
        insertKafka['channel_type'] = 'kafka'
        insertKafka['topic'] = prefix_topic2+fillData['topic']
        insertKafka['active'] = fillData['kafka']
        comChannelController.add(insertKafka)

def communication_update(fillData):
    if 'http-post' in fillData :
        query = {
            'channel_code': 'http-post-'+fillData['token_access']
        }
        commdata = comChannelController.findOne(query)['data']
        if commdata :
            if commdata['active'] != fillData['http-post'] : 
                updateHttp = query
                updateHttp['active'] = fillData['http-post']
                updateHttp['channel_type'] = 'http-post'
                updateHttp['topic'] = commdata['topic']
                comChannelController.update(query,updateHttp)

    if 'mqtt' in fillData :
        query = {
            'channel_code': 'mqtt-'+fillData['token_access']
        }
        commdata = comChannelController.findOne(query)['data']
        if commdata :
            if commdata['active'] != fillData['mqtt'] : 
                updateMqtt = query
                updateMqtt['active'] = fillData['mqtt']
                updateMqtt['channel_type'] = 'mqtt'
                updateMqtt['topic'] = commdata['topic']
                comChannelController.update(query,updateMqtt)

    if 'nats' in fillData :
        query = {
            'channel_code': 'nats-'+fillData['token_access']
        }
        commdata = comChannelController.findOne(query)['data']
        if commdata :
            if commdata['active'] != fillData['nats'] : 
                updateNats = query
                updateNats['active'] = fillData['nats']
                updateNats['channel_type'] = 'nats'
                updateNats['topic'] = commdata['topic']
                comChannelController.update(query,updateNats)

    if 'kafka' in fillData :
        query = {
            'channel_code': 'kafka-'+fillData['token_access']
        }
        commdata = comChannelController.findOne(query)['data']
        if commdata :
            if commdata['active'] != fillData['kafka'] : 
                updateKafka = query
                updateKafka['active'] = fillData['kafka']
                updateKafka['channel_type'] = 'kafka'
                updateKafka['topic'] = commdata['topic']
                comChannelController.update(query,updateKafka)

