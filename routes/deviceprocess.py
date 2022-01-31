import sys
from numpy import insert
sys.path.append('../')
from tornado.web import RequestHandler
from bson import ObjectId
import json 
from function import *
from controller import deviceController
from controller import deviceController
from controller import commETLController
from controller import groupSensorController
from controller import sensorController
from datetime import datetime

from configparser import ConfigParser
config = ConfigParser()
config.read("config.ini")

from datetime import timedelta
td = timedelta(hours=int(config["SERVER"]["timediff"]))

groups = []

#PRIMARY VARIABLE - DONT DELETE
define_url = [
    ['add/','add'],
    ['edit/','update'],
    ['delete/','delete'],
    ['batch/([^/]+)/','batch']
]

class add(RequestHandler):
  def post(self):    
    data = json.loads(self.request.body)    
    if 'device_code' not in data:
        response = {"status":False, "message":"Parameter device_code not exists",'data':json.loads(self.request.body)}               
        self.write(response)
        return
    if 'field' not in data:
        response = {"status":False, "message":"Parameter field not exists",'data':json.loads(self.request.body)}               
        self.write(response)
        return
    if 'pre' not in data:
        response = {"status":False, "message":"Parameter pre not exists",'data':json.loads(self.request.body)}               
        self.write(response)
        return
    if 'process' not in data:
        response = {"status":False, "message":"Parameter process not exists",'data':json.loads(self.request.body)}               
        self.write(response)
        return
    if 'var' not in data:
        response = {"status":False, "message":"Parameter var not exists",'data':json.loads(self.request.body)}               
        self.write(response)
        return
    
    #check if exist
    query = {"device_code":data['device_code']}
    updateData = {}
    device = deviceController.findOne(query)
    if not device['status']:
        response = {"status":False, "message":"Device not found",'data':json.loads(self.request.body)}               
        self.write(response)
        return
    device = device['data']
    if 'field_process' not in device:
        device['field_process'] = {}                    

    device['field_process'][data['field']] = {
        'pre':data['pre'],
        'process':data['process'],
        'var':data['var']
    }
    updateData['field_process'] = device['field_process']
    try:
        query = {"_id" : ObjectId(device["id"]) }        
    except:
        response = {"status":False, "message":"Failed to add", 'data':json.loads(self.request.body)}    
        return            
    update = deviceController.update(query,updateData)    
    if not update['status']:
        response = {"status":False, "message":"Failed to add", 'data':json.loads(self.request.body)}               
    else:
        response = {'message':'Success','status':True}    
    self.write(response)

class update(RequestHandler):
  def post(self):        
    data = json.loads(self.request.body)    
    if 'device_code' not in data:
        response = {"status":False, "message":"Parameter device_code not exists",'data':json.loads(self.request.body)}               
        self.write(response)
        return
    if 'oldfield' not in data:
        response = {"status":False, "message":"Parameter oldfield not exists",'data':json.loads(self.request.body)}               
        self.write(response)
        return
    if 'field' not in data:
        response = {"status":False, "message":"Parameter field not exists",'data':json.loads(self.request.body)}               
        self.write(response)
        return
    if 'pre' not in data:
        response = {"status":False, "message":"Parameter pre not exists",'data':json.loads(self.request.body)}               
        self.write(response)
        return
    if 'process' not in data:
        response = {"status":False, "message":"Parameter process not exists",'data':json.loads(self.request.body)}               
        self.write(response)
        return
    if 'var' not in data:
        response = {"status":False, "message":"Parameter var not exists",'data':json.loads(self.request.body)}               
        self.write(response)
        return
    
    #check if exist
    query = {"device_code":data['device_code']}
    updateData = {}
    device = deviceController.findOne(query)
    if not device['status']:
        response = {"status":False, "message":"Device not found",'data':json.loads(self.request.body)}               
        self.write(response)
        return
    device = device['data']
    if 'field_process' not in device:
        response = {"status":False, "message":"Data Not Found",'data':json.loads(self.request.body)}     
        self.write(response)
        return               

    if data['oldfield'] in device['field_process']:
        del device['field_process'][data['oldfield']]

    device['field_process'][data['field']] = {
        'pre':data['pre'],
        'process':data['process'],
        'var':data['var']
    }

    updateData['field_process'] = device['field_process']
    try:
        query = {"_id" : ObjectId(device["id"]) }        
    except:
        response = {"status":False, "message":"Failed to add", 'data':json.loads(self.request.body)}    
        return 
    update = deviceController.update(query,updateData)    
    if not update['status']:
        response = {"status":False, "message":"Failed to update", 'data':json.loads(self.request.body)}               
    else:
        response = {'message':'Update Success','status':True}    
    self.write(response)

class delete(RequestHandler):
  def post(self):        
    data = json.loads(self.request.body)    
    if 'device_code' not in data:
        response = {"status":False, "message":"Parameter device_code not exists",'data':json.loads(self.request.body)}               
        self.write(response)
        return
    if 'field' not in data:
        response = {"status":False, "message":"Parameter field not exists",'data':json.loads(self.request.body)}               
        self.write(response)
        return
    
    #check if exist
    query = {"device_code":data['device_code']}
    updateData = {}
    device = deviceController.findOne(query)
    if not device['status']:
        response = {"status":False, "message":"Device not found",'data':json.loads(self.request.body)}               
        self.write(response)
        return
    device = device['data']
    if 'field_process' not in device:
        response = {"status":False, "message":"Data Not Found",'data':json.loads(self.request.body)}     
        self.write(response)
        return               

    if data['field'] not in device['field_process']:
        response = {"status":False, "message":"Data Not Found",'data':json.loads(self.request.body)}     
        self.write(response)
        return  
        
    del device['field_process'][data['field']]
    updateData['field_process'] = device['field_process']
    try:
        query = {"_id" : ObjectId(device["id"]) }        
    except:
        response = {"status":False, "message":"Failed to add", 'data':json.loads(self.request.body)}    
        return 
    update = deviceController.update(query,updateData)     
    if not update['status']:
        response = {"status":False, "message":"Failed to delete", 'data':json.loads(self.request.body)}               
    else:
        response = {'message':'Delete Success','status':True}    
    self.write(response)

    
class batch(RequestHandler):
  def post(self,device):
    data = json.loads(self.request.body)
    query = {"device_code":device}
    deviceData = deviceController.findOne(query)
    if not deviceData['status']:
        response = {"status":False, "message":"Device not found",'data':json.loads(self.request.body)}               
    if 'field_process' not in deviceData["data"]:
        response = {"status":False, "message":"Process for Device not found",'data':json.loads(self.request.body)}               
    else:
        deviceData = deviceData["data"]
        deviceProcess = deviceData['field_process']
        if 'date_start' not in data:
            response = {"status":False, "message":"Date Start Not Found",'data':json.loads(self.request.body)}               
            self.write(response)
            return
        if 'date_end' not in data:
            response = {"status":False, "message":"Date End Not Found",'data':json.loads(self.request.body)}               
            self.write(response)
            return 

        if deviceData['group_code_name'] != "other":
            query = {"code_name":deviceData['group_code_name']}
            groupData = groupSensorController.findOne(query)
            if not groupData['status']:
                response = {"status":False, "message":"Device Not Found",'data':json.loads(self.request.body)} 
                self.write(response)
                return
            else:
                groupData = groupData['data']
                collection = 'sensor_data_'+groupData['id']
        else:
            collection = 'sensor_data_'+deviceData['device_code']
        
        datesrc_str = datetime.strptime(str(data['date_start'])+":00",'%Y-%m-%d %H:%M:%S') - td
        datesrc_end = datetime.strptime(str(data['date_end'])+":59",'%Y-%m-%d %H:%M:%S') - td
        query = {
           'date_add_server': {"$gte":datesrc_str, "$lt":datesrc_end },
           'device_code':device
        }
        result = sensorController.find(collection,query)
        insertCount = 0
        if result['status']:
            for document in result['data']:
                updateData = {}
                for fieldkey in deviceProcess:
                    fielditem = deviceProcess[fieldkey]
                    updateData[fieldkey] = commETLController.preproces(document,fielditem)
                try:                        
                    if "_id" in document:
                        query = {"_id" : ObjectId(document["_id"]) }
                    else:
                        query = {"_id" : ObjectId(document["id"]) }
                    update = sensorController.update(collection,query,updateData)     
                    if update['status']:
                        insertCount = insertCount + 1
                except:
                    print("Error")

        response = {"status":True,"data":{"insert_count":insertCount}}
    self.write(response)        
    
