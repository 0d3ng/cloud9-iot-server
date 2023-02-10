#!/usr/bin/python3

import sys
from bson import ObjectId
import json 
from function import *
from datetime import datetime,timedelta
from controller import comChannelController
from controller import deviceController
from pytz import timezone

sensors = []
db = db.dbmongo()

collection = "edgeconfig"

def add(fillData):  
    # insertQuery = {
    #     'edgeconfig_code':fillData.get('edgeconfig_code', None),
    #     'device_code':fillData.get('device_code', None),
    #     'method':fillData.get('method', None),
    #     'interface':fillData.get('interface', None),
    #     'string_sample':fillData.get('string_sample', None),
    #     'delimeter':fillData.get('delimeter', None), #arraylist [dem1,dem2]
    #     'string_pattern':fillData.get('string_pattern', None),
    #     'object_used':fillData.get('object_used', None),
    #     'active':fillData.get('active', False),                        
    #     'date_add': datetime.utcnow(),
    #     'add_by':fillData.get('add_by', None)             
    # }

    insertQuery = {
        'edgeconfig_code':fillData.get('edgeconfig_code', None),
        'device_code':fillData.get('device_code', None),
        'resource':fillData.get('resource', None),
        'interface':fillData.get('interface', None),
        'data_transmitted':fillData.get('data_transmitted', None),
        'time_interval':fillData.get('time_interval', None),
        'comm_service':fillData.get('comm_service', None),
        'local_data':fillData.get('local_data', None),
        'visualization':fillData.get('visualization', None),
        'active':fillData.get('active', False), 
        'date_add': datetime.utcnow(), 
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
    if 'edgeconfig_code' in query: queryUpdate['edgeconfig_code'] = query['edgeconfig_code']
    if '_id' in query: queryUpdate['_id'] = query['_id']
    
    if 'device_code' in data: updateData['device_code'] = data['device_code']
    if 'resource' in data: updateData['resource'] = data['resource']
    if 'interface' in data: updateData['interface'] = data['interface']
    if 'data_transmitted' in data: updateData['data_transmitted'] = data['data_transmitted']
    if 'time_interval' in data: updateData['time_interval'] = data['time_interval']
    if 'comm_service' in data: updateData['comm_service'] = data['comm_service']    
    
    if 'local_data' in data: updateData['local_data'] = data['local_data']    
    if 'visualization' in data: updateData['visualization'] = data['visualization']    
    
    # if 'method' in data: updateData['method'] = data['method']
    # if 'string_sample' in data: updateData['string_sample'] = data['string_sample']
    # if 'delimeter' in data: updateData['delimeter'] = data['delimeter']
    # if 'string_pattern' in data: updateData['string_pattern'] = data['string_pattern']
    # if 'object_used' in data: updateData['object_used'] = data['object_used']
    
    if 'active' in data: updateData['active'] = data['active']
    if 'updated_by' in data: updateData['updated_by'] = data['updated_by']
    if 'date_download' in data: 
        try:
            updateData['date_download'] = cloud9Lib.cv2datetime(data['date_download'])
        except:
            updateData['date_download'] = datetime.now(timezone('Asia/Tokyo'))
    
    if 'edge_device_id' in data: updateData['edge_device_id'] = data['edge_device_id']

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


def config_file(id,config):
    query = {
        'edgeconfig_code':id
    }
    edge_data = findOne(query)
    if edge_data['status'] == False:
        return {"status":False, "message":"No Configuration Data"}
    edge_data= edge_data["data"]
    query = {
        'device_code':edge_data["device_code"]
    }
    device_data = deviceController.findOne(query)
    if device_data['status'] == False:
        return {"status":False, "message":"No Device Data"}
    device_data = device_data["data"]

    #communication service
    if( edge_data["comm_service"] == "mqtt" and device_data["communication"]["mqtt"] == True):
        mqtt_server = device_data["communication"]["server"]
        if(mqtt_server == "localhost"):
            mqtt_server = config["MQTT"]["edge_server"]
        comm_service = {
            "mqtt":{
                "server": mqtt_server,
			    "port": device_data["communication"]["port"],
			    "topic": device_data["communication"]["topic"]
            }
        }
    elif( edge_data["comm_service"] == "http_post" and device_data["communication"]["http-post"] == True ):
        api_server = config["SERVER"]["ip"]
        port = config["SERVER"]["port"]
        comm_service = {
            "http_post":api_server+":"+port+"/comdata/sensor/"+device_data["key_access"]
        }
    
    if("local_data" in edge_data):
        local_data = edge_data["local_data"]
    else:
        local_data = []
    
    
    if("visualization" in edge_data):
        visualization = edge_data["visualization"]
    else:
        visualization = []

    edge_config = {
        "device_code": device_data["device_code"],
        "configuration_code":edge_data["edgeconfig_code"],
        "device_info": {
            "name": device_data["name"],
            "field": device_data["field"]
        },
        "resource":edge_data["resource"],
        "interface": edge_data["interface"],
        "data_transmitted":edge_data["data_transmitted"],
        "time_interval":edge_data["time_interval"],
        "communication_protocol": comm_service,
        "local_data":local_data,
        "visualization":visualization
    }
    
    response = {'status':True,'message':'Success','data':edge_config}
    return cloud9Lib.jsonObject(response)


#====================================================================

#FOR SERIAL COMM Network Interface
from detect_delimiter import detect

def detect_delimeter(raw):
    raw = str(raw)
    raw = raw.strip()
    delimeter = detect(raw)
    return delimeter

def covert_to_list(raw,delimeter=None):
    raw = str(raw)
    raw = raw.strip()
    if(delimeter == None):
        delimeter = detect_delimeter(raw)  
        if(delimeter == None):            
            return False,None                      
    x = raw.split(delimeter)
    return x,delimeter

def convert_to_json(raw,delimeter1=None,delimeter2=None): #only 2 dimension array
    item,dem = covert_to_list(raw,delimeter1)
    if(item == False):
        return False,delimeter1,delimeter2
    json_object = {}
    for i in item:
        if len(i)<1:
            continue
        value,dem2 = covert_to_list(i,delimeter2)
        if(value == False):
            continue
        if(len(value)>1):
            json_object[ value[0] ] = value[1]
    return json_object,dem,dem2 