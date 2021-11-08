#!/usr/bin/python3

import sys
from bson import ObjectId
import json 
from function import *
import datetime


sensors = []
db = db.dbmongo()

collection = "communication_channel"

def add(fillData):  
    insertQuery = {
        'channel_code':fillData.get('channel_code', None),
        'token_access':fillData.get('token_access', None),
        'topic':fillData.get('topic', None), #array[location, detail, purpose]
        'channel_type':fillData.get('channel_type', None), #array id group
        'active':fillData.get('active', False),
        'collection_name':fillData.get('collection_name', None),
        'date_add': datetime.datetime.utcnow(),
        'index_log':fillData.get('index_log', None),
        'add_by':fillData.get('add_by', None)        
    }
    # print("------------------")
    # sys.stdout.flush()
    result = db.insertData(collection,insertQuery)
    if result == []:
        response = {'status':False, 'message':"Add Failed"}               
    else:
        response = {'status':True,'message':'Success','data':result}
        if insertQuery['active'] == True and ( insertQuery['channel_type'] == 'mqtt' or  insertQuery['channel_type'] == 'nats'  or  insertQuery['channel_type'] == 'kafka' ):
            trigger(insertQuery['channel_type'],insertQuery['topic'],insertQuery['channel_code'],'active')

    return cloud9Lib.jsonObject(response)

def find(query):  
    result = db.find(collection,query)
    print("------------------")
    print(query)
    print("------------------")
    sys.stdout.flush()
    if result == []:
        response = {"status":False, "data":query}               
    else:
        response = {'status':True, 'data':result}    
    return cloud9Lib.jsonObject(response)

def findOne(query):  
    result = db.findOne(collection,query, None)
    if result is None:
        response = {"status":False, "data":query}               
    else:
        response = {'status':True,'message':'Success','data':result}    
    return cloud9Lib.jsonObject(response)

def update(query,data):            
    updateData = {}
    queryUpdate = {}
    if 'channel_code' in query: queryUpdate['channel_code'] = query['channel_code']
    if '_id' in query: queryUpdate['_id'] = query['_id']

    if 'token_access' in data: updateData['token_access'] = data['token_access']
    if 'topic' in data: updateData['topic'] = data['topic']
    if 'channel_type' in data: updateData['channel_type'] = data['channel_type']
    if 'collection_name' in data: updateData['collection_name'] = data['collection_name']
    if 'active' in data: updateData['active'] = data['active']
    
    last = findOne(queryUpdate)['data']
    
    if updateData == {}:
        return {"status":False, "message":"UPDATE NONE"}        
    
    result = db.updateData(collection,queryUpdate,updateData)
    if not result :
        response = {"status":False, "message":"UPDATE FAILED"}               
    else:
        response = {'status':True,'message':'Success','data':result}
        if last['active'] !=  updateData['active']:
            if updateData['active'] == True and ( updateData['channel_type'] == 'mqtt' or  updateData['channel_type'] == 'nats' or  updateData['channel_type'] == 'kafka' ):
                trigger(updateData['channel_type'],updateData['topic'],last['channel_code'],'active')

            if updateData['active'] == False and ( updateData['channel_type'] == 'mqtt' or  updateData['channel_type'] == 'nats' or  updateData['channel_type'] == 'kafka' ):
                trigger(updateData['channel_type'],updateData['topic'],last['channel_code'],'nonactive')

    return cloud9Lib.jsonObject(response)

def delete(query):            
    result = db.deleteData(collection,query)
    if not result:
        response = {"status":False, "message":"DELETE FAILED"}               
    else:
        response = {'status':True,'message':'Success','data':result}
    return cloud9Lib.jsonObject(response)

def trigger(channel_type,topic,channel_code,status):
    print(channel_type)
    print(topic)
    sys.stdout.flush()
    if channel_type == 'mqtt':        
        send = {
            'topic':topic,
            'channel_code':channel_code
        }
        if status == 'active':
            mqttcom.publish("mqtt/service/subscribe",send)
            return
        else:
            mqttcom.publish("mqtt/service/unsubscribe",send)
            return

    if channel_type == 'nats':        
        send = {
            'topic':topic,
            'channel_code':channel_code
        }
        if status == 'active':
            natscom.publish("nats/service/subscribe",send)
            return
        else:
            natscom.publish("nats/service/unsubscribe",send)
            return

    if channel_type == 'kafka':  
        send = {
            'topic':topic,
            'channel_code':channel_code
        }
        if status == 'active':
            kafkacom.publish("kafka-service-subscribe",send)
            return
        else:
            kafkacom.publish("kafka-service-unsubscribe",send)
            return

