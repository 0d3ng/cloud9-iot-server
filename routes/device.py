import sys
sys.path.append('../')
from tornado.web import RequestHandler
from bson import ObjectId
import json 
from function import *
from controller import deviceController
from controller import groupSensorController
from controller import sensorController

groups = []

#PRIMARY VARIABLE - DONT DELETE
define_url = [
    ['add/','add'],
    ['','list'],
    ['count/','count'],
    ['detail/','detail'],
    ['edit/','update'],
    ['delete/','delete'],
    ['data/([^/]+)/','getdata'],
    # ['data/([^/]+)/update/','updatedata'],
    # ['data/([^/]+)/delete/','deletedata'],
]

class add(RequestHandler):
  def post(self):    
    data = json.loads(self.request.body)
    print(data)
    sys.stdout.flush()
    if 'group_code_name' not in data:
        response = {"status":False, "message":"Parameter group_code_name not exists",'data':json.loads(self.request.body)}               
        self.write(response)
        return
    #check if exist
    query = {"code_name":data['group_code_name']}
    result = groupSensorController.findOne(query)
    if not result['status']:
        response = {"status":False, "message":"Group not found",'data':json.loads(self.request.body)}               
        self.write(response)
        return


    if 'key_access' not in data:
        data['key_access'] = generateAccess();
    else:
        if checkKeyAccess(data['key_access']):
            response = {"status":False, "message":"Key access is exits",'data':json.loads(self.request.body)} 
            self.write(response)
            return


    if 'device_code' not in data:
        data['device_code'] = generateCode(data['group_code_name']);

    insert = deviceController.add(data)    
    if not insert['status']:
        response = {"status":False, "message":"Failed to add", 'data':json.loads(self.request.body)}               
    else:
        response = {'message':'Success','status':True}    
    self.write(response)

class list(RequestHandler):
  def post(self):    
    data = json.loads(self.request.body)
    query = data
    result = deviceController.find(query)
    if not result['status']:
        response = {"status":False, "message":"Data Not Found",'data':json.loads(self.request.body)}               
    else:
        response = {"status":True, 'message':'Success','data':result['data']}
    self.write(response)

class count(RequestHandler):
  def post(self):    
    data = json.loads(self.request.body)
    query = data
    if "id" in query :
        try:
            query["_id"] = ObjectId(query["id"])
            del query["id"]
        except:
            del query["id"]
            
    query = data
    result = deviceController.find(query)
    if not result['status']:
        response = {"status":False, "message":"Data Not Found",'data':0}               
    else:
        response = {"status":True, 'message':'Success','data':len(result['data'])}
    self.write(response)

class detail(RequestHandler):
  def post(self):    
    data = json.loads(self.request.body)
    query = data
    result = deviceController.findOne(query)    
    # print(remote_ip)
    # print("------------------")
    # sys.stdout.flush()
    if not result['status']:
        response = {"status":False, "message":"Data Not Found",'data':json.loads(self.request.body)}               
    else:
        response = {"status":True, 'message':'Success','data':result['data']}
    self.write(response)

class update(RequestHandler):
  def post(self):        
    data = json.loads(self.request.body)
    if 'id' not in data:
        response = {"status":False, "message":"Id Not Found",'data':json.loads(self.request.body)}               
        self.write(response)
        return

    try:
        query = {"_id":ObjectId(data["id"])}
    except:
        response = {"status":False, "message":"Wrong id",'data':json.loads(self.request.body)}               
        self.write(response) 
        return

    if 'key_access' in data:
        if checkKeyAccess(data['key_access'],query['_id']):
            response = {"status":False, "message":"Key access is exits",'data':json.loads(self.request.body)} 
            self.write(response)
            return


    result = deviceController.findOne(query)
    if not result['status']:
        response = {"status":False, "message":"Data Not Found",'data':json.loads(self.request.body)}               
    else:
        update = deviceController.update(query,data)
        if not update['status']:
            response = {"status":False, "message":"Failed to update","data":json.loads(self.request.body)}
        else:
            response = {"status":True, 'message':'Update Success'}
    self.write(response)

class delete(RequestHandler):
  def post(self):        
    data = json.loads(self.request.body)
    if 'id' not in data:
        response = {"status":False, "message":"Id Not Found",'data':json.loads(self.request.body)}               
        self.write(response)
        return

    try:
        query = {"_id":ObjectId(data["id"])}
    except:
        response = {"status":False, "message":"Wrong id",'data':json.loads(self.request.body)}               
        self.write(response) 
        return
    
    result = deviceController.findOne(query)
    if not result['status']:
        response = {"status":False, "message":"Data Not Found",'data':json.loads(self.request.body)}            
    else:
        delete = deviceController.delete(query)
        if not delete['status']:
            response = {"status":False, "message":"Failed to delete","data":json.loads(self.request.body)}
        else:
            response = {"status":True, 'message':'Delete Success'}
    self.write(response)

class getdata(RequestHandler):
  def post(self,device):    
    data = json.loads(self.request.body)
    query = {"device_code":device}
    deviceData = deviceController.findOne(query)
    if not deviceData['status']:
        response = {"status":False, "message":"Device Not Found",'data':json.loads(self.request.body)}               
    else:
        deviceData = deviceData['data']
        query = {"code_name":deviceData['group_code_name']}
        groupData = groupSensorController.findOne(query)
        if not groupData['status']:
            response = {"status":False, "message":"Device Not Found",'data':json.loads(self.request.body)} 
        else:        
            groupData = groupData['data']
            limit =  None
            skip = None
            sort = ('date_add_server',-1)
            if 'limit' in data:
                limit = data['limit']
                del data['limit']
                if 'page_num' in data:
                    page_num = data['page_num']
                    del data['page_num']
                else:
                    page_num = 1
                skip = limit * (page_num - 1)
            if 'sort' in data:
                sort = (data['sort']['field'],data['sort']['type'])            
            query = data
            exclude = {'raw_message':0}
            collection = 'sensor_data_'+groupData['id']
            result = sensorController.find(collection,query,exclude,limit,skip,sort)
            response = {"status":True, 'message':'Success','data':result['data']}    
    self.write(response)

def checkKeyAccess(key,execpt=""):
    if execpt:
        query = {"key_access":key,"_id":{ '$ne' : execpt } }
        result = deviceController.findOne(query)
    else:
        query = {"key_access":key}
        result = deviceController.findOne(query)
    
    print(result)

    if result['status']:
        return True
    else:
        return False

def generateCode(code):
    code = code+"-"+cloud9Lib.randomOnlyString(2)+cloud9Lib.randomNumber(2)
    #check if exist
    query = {"device_code":code}
    result = deviceController.findOne(query)
    if result['status']:
        return generateCode(code)
    else:
        return code

def generateAccess():
    code = cloud9Lib.randomStringLower(16)
    print(code)

    result = checkKeyAccess(code)
    
    print(result)
    print("------------------")
    sys.stdout.flush()
    if result:
        return generateAccess()
    else:
        return code