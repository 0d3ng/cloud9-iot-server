import sys
sys.path.append('../')
from tornado.web import RequestHandler
from bson import ObjectId
import json 
from function import *
from controller import deviceController
from controller import groupSensorController
from controller import sensorController
from controller import edgeController
from controller import userController
from controller import commETLController
from datetime import datetime
from pytz import timezone
from functools import wraps
import jwt

from configparser import ConfigParser
config = ConfigParser()
config.read("config.ini")

from datetime import timedelta
td = timedelta(hours=int(config["SERVER"]["timediff"]))
prefix_collection = "sensor_data_"
SECRET_KEY = config["JWT"]["secret_key"]

groups = []


#PRIMARY VARIABLE - DONT DELETE
define_url = [
    ['data/([^/]+)/','getSensorData'],
    ['data/([^/]+)/count/','countSensorData'],
    ['data/([^/]+)/detail/','detailSensorData'],
    ['data/([^/]+)/add/','addSensorData'],
    ['data/([^/]+)/edit/','updateSensorData'],
    ['data/([^/]+)/delete/','deleteSensorData'],
    ['data/([^/]+)','getSensorData'],
    ['data/([^/]+)/count','countSensorData'],
    ['data/([^/]+)/detail','detailSensorData'],
    ['data/([^/]+)/add','addSensorData'],
    ['data/([^/]+)/edit','updateSensorData'],
    ['data/([^/]+)/delete','deleteSensorData']
]

def token_required(handler_function):
    @wraps(handler_function)
    def decorator(self, *args, **kwargs):
        token = self.request.headers.get('Authorization').split(' ')[1]        
        if not token:
            self.set_status(401)
            self.write({"status":False,'message': 'Token is missing'})
            return
        try:
            jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        except jwt.ExpiredSignatureError:
            self.set_status(401)
            self.write({"status":False,'message': 'Token has expired'})
            return
        except jwt.InvalidTokenError:
            self.set_status(401)
            self.write({"status":False,'message': 'Invalid token'})
            return
        return handler_function(self, *args, **kwargs)
    return decorator

class getSensorData(RequestHandler):
  @token_required
  def post(self,device):    
    data = json.loads(self.request.body)
    response = ""
    query = {"device_code":device}
    deviceData = deviceController.findOne(query)
    if not deviceData['status']:
        response = {"status":False, "message":"Device Not Found",'data':json.loads(self.request.body)}               
    else:
        deviceData = deviceData['data']
        if deviceData['group_code_name'] != "other":
            query = {"code_name":deviceData['group_code_name']}
            groupData = groupSensorController.findOne(query)
            if not groupData['status']:
                response = {"status":False, "message":"Device Not Found",'data':json.loads(self.request.body)} 
            else:
                groupData = groupData['data']
                collection = prefix_collection+groupData['id']
        else:
            collection = prefix_collection+deviceData['device_code']
        
        if response == "":
            limit =  None
            skip = None
            sort = ('date_add_server',-1)
            if 'limit' in data:
                limit = data['limit']
                del data['limit']
                if 'page_number' in data:
                    page_num = data['page_number']
                    del data['page_number']
                else:
                    page_num = 1
                skip = limit * (page_num - 1)
            if 'skip' in data:
                skip = data['skip']
                del data['skip']
            if 'sort' in data:
                sort = (data['sort']['field'],data['sort']['type'])   
                del data['sort']         
            if 'date' in data:
                date_time_str = str(data['date'])
                datesrc_str = datetime.strptime(date_time_str+" 00:00:00",'%Y-%m-%d %H:%M:%S') - td
                datesrc_end = datetime.strptime(date_time_end+" 23:59:59",'%Y-%m-%d %H:%M:%S') - td
                data['date_add_server'] = {"$gte":datesrc_str, "$lt":datesrc_end }
                del data['date']
            if 'date_start' in data and 'date_end' in data:
                date_time_str = str(data['date_start'])
                date_time_end = str(data['date_end'])
                if 'time_start'in data and 'time_end' in data:
                    datesrc_str = datetime.strptime(date_time_str+" "+str(data['time_start']),'%Y-%m-%d %H:%M:%S') - td
                    datesrc_end = datetime.strptime(date_time_end+" "+str(data['time_end']),'%Y-%m-%d %H:%M:%S') - td
                    del data['time_start']
                    del data['time_end']
                else:
                    datesrc_str = datetime.strptime(date_time_str+" 00:00:00",'%Y-%m-%d %H:%M:%S') - td
                    datesrc_end = datetime.strptime(date_time_end+" 23:59:59",'%Y-%m-%d %H:%M:%S') - td
                data['date_add_server'] = {"$gte":datesrc_str, "$lt":datesrc_end }
                del data['date_start']
                del data['date_end']
            query = data
            query["device_code"] = device
            exclude = {'raw_message':0}
            result = sensorController.find(collection,query,exclude,limit,skip,sort,True)
            if not result['status']:
                response = {"status":False, "message":"Data Not Found",'data':json.loads(self.request.body)}               
            else:
                response = {"status":True, 'message':'Success','data':result['data']}
    self.write(response)

class countSensorData(RequestHandler):
  @token_required
  def post(self,device):    
    data = json.loads(self.request.body)
    response = ""
    query = {"device_code":device}
    deviceData = deviceController.findOne(query)
    if not deviceData['status']:
        response = {"status":False, "message":"Device Not Found",'data':json.loads(self.request.body)}               
    else:
        deviceData = deviceData['data']
        if deviceData['group_code_name'] != "other":
            query = {"code_name":deviceData['group_code_name']}
            groupData = groupSensorController.findOne(query)
            if not groupData['status']:
                response = {"status":False, "message":"Device Not Found",'data':json.loads(self.request.body)} 
            else:
                groupData = groupData['data']
                collection = prefix_collection+groupData['id']
        else:
            collection = prefix_collection+deviceData['device_code']
        
        if response == "":
            limit =  None
            skip = None
            sort = ('date_add_server',-1)
            if 'limit' in data:
                limit = data['limit']
                del data['limit']
                if 'page_number' in data:
                    page_num = data['page_number']
                    del data['page_number']
                else:
                    page_num = 1
                skip = limit * (page_num - 1)
            if 'skip' in data:
                skip = data['skip']
                del data['skip']
            if 'sort' in data:
                sort = (data['sort']['field'],data['sort']['type'])            
                del data['sort']         
            if 'date' in data:
                date_time_str = str(data['date'])
                datesrc_str = datetime.strptime(date_time_str+" 00:00:00",'%Y-%m-%d %H:%M:%S') - td
                datesrc_end = datetime.strptime(date_time_end+" 23:59:59",'%Y-%m-%d %H:%M:%S') - td
                data['date_add_server'] = {"$gte":datesrc_str, "$lt":datesrc_end }
                del data['date']
            if 'date_start' in data and 'date_end' in data:
                date_time_str = str(data['date_start'])
                date_time_end = str(data['date_end'])
                if 'time_start'in data and 'time_end' in data:
                    datesrc_str = datetime.strptime(date_time_str+" "+str(data['time_start']),'%Y-%m-%d %H:%M:%S') - td
                    datesrc_end = datetime.strptime(date_time_end+" "+str(data['time_end']),'%Y-%m-%d %H:%M:%S') - td
                    del data['time_start']
                    del data['time_end']
                else:
                    datesrc_str = datetime.strptime(date_time_str+" 00:00:00",'%Y-%m-%d %H:%M:%S') - td
                    datesrc_end = datetime.strptime(date_time_end+" 23:59:59",'%Y-%m-%d %H:%M:%S') - td
                data['date_add_server'] = {"$gte":datesrc_str, "$lt":datesrc_end }
                del data['date_start']
                del data['date_end']
            query = data
            query["device_code"] = device
            exclude = {'raw_message':0}
            result = sensorController.count(collection,query,exclude,limit,skip,sort)
            if not result['status']:
                response = {"status":False, "message":"Data Not Found",'data':0}               
            else:
                response = {"status":True, 'message':'Success','data':result['data']}
    self.write(response)

class detailSensorData(RequestHandler):
  @token_required
  def post(self,device_code):    
    data = json.loads(self.request.body)    
    if not device_code:
        response = {"status":False, "message":"Device Code not found",'data':json.loads(self.request.body)}               
        self.write(response)
        return
    query = {"device_code":device_code}
    deviceData = deviceController.findOne(query)
    if not deviceData['status']:
        response = {"status":False, "message":"Device Not Found",'data':json.loads(self.request.body)}      
        self.write(response)
        return             
    deviceData = deviceData['data']
    collection = prefix_collection+deviceData['device_code']
    if "_id" in data :
        try:
            data["_id"] = ObjectId(data["_id"])
        except:
            data["_id"] = data["_id"]   
    sort = ('date_add_auto',-1)
    exclude = {'raw_message':0}
    if 'sort' in data:
        sort = (data['sort']['field'],data['sort']['type'])            
        del data['sort']
    query = data
    
    result = sensorController.findOne(collection,query,exclude,sort,True)    
    
    if not result['status']:
        response = {"status":False, "message":"Data Not Found",'data':json.loads(self.request.body)}               
    else:
        # if 'raw_message'in result['data'] :
        #     del result['data']['raw_message']
        if result['data'] == None:
            result['data'] = []
        response = {"status":True, 'message':'Success','data':result['data']}
    self.write(response)

class addSensorData(RequestHandler):
  @token_required
  def post(self,device_code):    
    data = json.loads(self.request.body)
    if not device_code:
        response = {"status":False, "message":"Device Code not found",'data':json.loads(self.request.body)}               
        self.write(response)
        return
    query = {"device_code":device_code}
    deviceData = deviceController.findOne(query)
    if not deviceData['status']:
        response = {"status":False, "message":"Device Not Found",'data':json.loads(self.request.body)}      
        self.write(response)
        return             
    deviceData = deviceData['data']
    collection = prefix_collection+deviceData['device_code']

    insert = commETLController.etl_inner(collection,"device-"+device_code,deviceData,device_code,data)
    if insert['status']:
        response = {'status':True, 'message':"Success", 'data':insert["data"]}
    else:
        response = {"status":False, "message":"Failed to add", 'data':json.loads(self.request.body)}

    #Save data using ETL
    #Return the inserted data as a Response.


    self.write(response)


class updateSensorData(RequestHandler):
  @token_required
  def post(self,device_code):    
    data = json.loads(self.request.body)
    if not device_code:
        response = {"status":False, "message":"Device Code not found",'data':json.loads(self.request.body)}               
        self.write(response)
        return
    
    if '_id' not in data:
        response = {"status":False, "message":"Id Not Found",'data':json.loads(self.request.body)}               
        self.write(response)
        return    

    query = {"device_code":device_code}
    deviceData = deviceController.findOne(query)
    if not deviceData['status']:
        response = {"status":False, "message":"Device Not Found",'data':json.loads(self.request.body)}      
        self.write(response)
        return             
    deviceData = deviceData['data']
    collection = prefix_collection+deviceData['device_code']

    try:
        query = {"_id":ObjectId(data["_id"])}
        del data["_id"]
    except:
        response = {"status":False, "message":"Wrong id",'data':json.loads(self.request.body)}               
        self.write(response) 
        return
    updateQuery = {}
    deviceData = deviceData['field']
    state = False
    for fieldData in deviceData:
        if type(fieldData) is dict:
            fieldName = list(fieldData.keys())[0]
        else:
            fieldName = fieldData
        updateQuery[fieldName],state = commETLController.extract_etl(fieldData,data,collection,device_code,state)
    if state:
        update = sensorController.update(collection,query,data)
        if update['status']:
            exclude = {'raw_message':0}
            sort = ('date_add_server',-1)
            result = sensorController.findOne(collection,query,exclude,sort,True)
            response = {'status':True, 'message':"Success", 'data':result['data']}
        else:
            response = {"status":False, "message":"Failed to update", 'data':json.loads(self.request.body)}
    else:
        response = {"status":False, "message":"Failed to update", 'data':json.loads(self.request.body)}
    self.write(response)

class deleteSensorData(RequestHandler):
  @token_required
  def post(self,device_code):        
    data = json.loads(self.request.body)
    if not device_code:
        response = {"status":False, "message":"Device Code not found",'data':json.loads(self.request.body)}               
        self.write(response)
        return
    if '_id' not in data:
        response = {"status":False, "message":"Id Not Found",'data':json.loads(self.request.body)}               
        self.write(response)
        return
    try:
        query = {"_id":ObjectId(data["_id"])}
    except:
        response = {"status":False, "message":"Wrong id",'data':json.loads(self.request.body)}               
        self.write(response) 
        return
    
    result = sensorController.findOne(prefix_collection+device_code,query)
    if not result['status']:
        response = {"status":False, "message":"Data Not Found",'data':json.loads(self.request.body)}            
    else:
        delete = sensorController.delete(prefix_collection+device_code,query)
        if not delete['status']:
            response = {"status":False, "message":"Failed to delete","data":json.loads(self.request.body)}
        else:
            response = {"status":True, 'message':'Delete Success'}
    self.write(response)
