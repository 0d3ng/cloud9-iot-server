#!/usr/bin/python3

import sys

from logger import setup_logger

logger = setup_logger(to_file=False)
from function import cloud9Lib, db, mqttcom, kafkacom, natscom
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
        'mqtt_username':fillData.get('mqtt_username', None),
        'mqtt_pass':fillData.get('mqtt_pass', None),
        'add_by':fillData.get('add_by', None)        
    }
    # print("------------------")
    # sys.stdout.flush()
    result = db.insertData(collection,insertQuery)
    if not result:
        response = {'status':False, 'message':"Add Failed"}               
    else:
        response = {'status':True,'message':'Success','data':result}
        if insertQuery['active'] == True and ( insertQuery['channel_type'] == 'mqtt' or  insertQuery['channel_type'] == 'nats'  or  insertQuery['channel_type'] == 'kafka' ):
            trigger(insertQuery['channel_type'],insertQuery['topic'],insertQuery['channel_code'],'active')

    return cloud9Lib.jsonObject(response)

def find(query):  
    result = db.find(collection,query)
    logger.info(result)
    logger.info("------------------")
    logger.info(query)
    logger.info("------------------")
    sys.stdout.flush()
    if not result:
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
    if 'mqtt_username' in data: updateData['mqtt_username'] = data['mqtt_username']
    if 'mqtt_pass' in data: updateData['mqtt_pass'] = data['mqtt_pass']
    
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
    result = db.deleteDataMany(collection,query)
    if not result:
        response = {"status":False, "message":"DELETE FAILED"}               
    else:
        response = {'status':True,'message':'Success','data':result}
    return cloud9Lib.jsonObject(response)

def trigger(channel_type,topic,channel_code,status):
    logger.info(channel_type)
    logger.info(topic)
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

def addOther(fillData):  
    insertQuery = {
        'channel_code':fillData.get('channel_code', None),
        'token_access':fillData.get('token_access', None),
        'server':fillData.get('server', None), 
        'port':fillData.get('port', None), 
        'topic':fillData.get('topic', None), 
        'channel_type':fillData.get('channel_type', None), 
        'active':fillData.get('active', False),
        'collection_name':fillData.get('collection_name', None),
        'device_code':fillData.get('device_code', None),
        'date_add': datetime.datetime.utcnow(),
        'index_log':fillData.get('index_log', None),        
        'mqtt_username':fillData.get('mqtt_username', None),
        'mqtt_pass':fillData.get('mqtt_pass', None),
        'add_by':fillData.get('add_by', None)        
    }
    logger.info("------------------")
    logger.info(insertQuery)
    logger.info("------------------")
    # sys.stdout.flush()
    result = db.insertData(collection,insertQuery)
    if result == []:
        response = {'status':False, 'message':"Add Failed"}               
    else:
        response = {'status':True,'message':'Success','data':result}
        if insertQuery['active'] == True and ( insertQuery['channel_type'] == 'mqtt'):
            if(insertQuery["port"] == "1885"):
                triggerOther(insertQuery['channel_type'],insertQuery['server'],insertQuery['port'],insertQuery['topic'],insertQuery['channel_code'],'active', insertQuery["mqtt_username"],insertQuery["mqtt_pass"])
            else:
                triggerOther(insertQuery['channel_type'],insertQuery['server'],insertQuery['port'],insertQuery['topic'],insertQuery['channel_code'],'active')

    return cloud9Lib.jsonObject(response)

def updateOther(query,data):            
    updateData = {}
    queryUpdate = {}
    if 'channel_code' in query: queryUpdate['channel_code'] = query['channel_code']
    if '_id' in query: queryUpdate['_id'] = query['_id']

    if 'token_access' in data: updateData['token_access'] = data['token_access']
    if 'server' in data: updateData['server'] = data['server']
    if 'port' in data: updateData['port'] = data['port']
    if 'topic' in data: updateData['topic'] = data['topic']
    if 'channel_type' in data: updateData['channel_type'] = data['channel_type']
    if 'collection_name' in data: updateData['collection_name'] = data['collection_name']
    if 'active' in data: updateData['active'] = data['active']
    if 'mqtt_username' in data: updateData['mqtt_username'] = data['mqtt_username']
    if 'mqtt_pass' in data: updateData['mqtt_pass'] = data['mqtt_pass']
    
    logger.info(queryUpdate)
    logger.info(updateData)
    sys.stdout.flush()
    last = findOne(queryUpdate)['data']
    if updateData == {}:
        return {"status":False, "message":"UPDATE NONE"}        
    
    result = db.updateData(collection,queryUpdate,updateData)
    if not result :
        response = {"status":False, "message":"UPDATE FAILED"}               
    else:
        response = {'status':True,'message':'Success','data':result}
        if last['active'] !=  updateData['active']:
            if updateData['active'] == True and ( updateData['channel_type'] == 'mqtt'):
                triggerOther(updateData['channel_type'],updateData['server'],updateData['port'],updateData['topic'],last['channel_code'],'active')

            if updateData['active'] == False and ( updateData['channel_type'] == 'mqtt'):
                triggerOther(updateData['channel_type'],updateData['server'],updateData['port'],updateData['topic'],last['channel_code'],'nonactive')

        if "server" in last and 'server' in updateData:
            if (last['server'] !=  updateData['server']) or (last['port'] !=  updateData['port']) or (last['topic'] !=  updateData['topic']):
                triggerOther(updateData['channel_type'],updateData['server'],updateData['port'],updateData['topic'],last['channel_code'],'nonactive')
                triggerOther(updateData['channel_type'],updateData['server'],updateData['port'],updateData['topic'],last['channel_code'],'active') 

    return cloud9Lib.jsonObject(response)

def deleteOther(query):
    listData = find(query)['data']            
    result = db.deleteDataMany(collection,query)
    if not result:
        response = {"status":False, "message":"DELETE FAILED"}               
    else:
        response = {'status':True,'message':'Success','data':result}
        for i in listData: 
            listItem = i
            if listItem['active'] == True and ( listItem['channel_type'] == 'mqtt'):
                    triggerOther(listItem['channel_type'],listItem['server'],listItem['port'],listItem['topic'],listItem['channel_code'],'nonactive')
    return cloud9Lib.jsonObject(response)

def triggerOther(channel_type,server,port,topic,channel_code,status, mqtt_username="*", mqtt_pass="*"):
    logger.info(channel_type)
    logger.info(server+":"+str(port)+" -> "+topic+" "+status)
    sys.stdout.flush()
    if channel_type == 'mqtt':        
        send = {
            'topic':topic,
            'server':server,
            'port':port,
            'channel_code':channel_code
        }
        if port == "1885":
            send["mqtt_username"] = mqtt_username
            send["mqtt_pass"] = mqtt_pass


        if status == 'active':
            mqttcom.publish("mqtt/service-other/subscribe",send)
            return
        else:
            mqttcom.publish("mqtt/service-other/unsubscribe",send)
            return

