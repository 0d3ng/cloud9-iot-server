import sys
sys.path.append('../')
from tornado.web import RequestHandler
from bson import ObjectId
import json 
from function import *
from controller import deviceController
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
    ['delete/','delete']
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

    
