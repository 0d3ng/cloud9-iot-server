import sys
sys.path.append('../')
from tornado.web import RequestHandler
from bson import ObjectId
import json 
from function import *
from controller import comChannelController

groups = []


#PRIMARY VARIABLE - DONT DELETE
define_url = [
    ['add/','add'],
    ['','list'],
    ['detail/','detail'],
    ['edit/','update'],
    ['delete/','delete']
]

class add(RequestHandler):
  def post(self):    
    data = json.loads(self.request.body)
    if 'channel_type' not in data:
        response = {"status":False, "message":"Channel Type Not Found",'data':json.loads(self.request.body)}               
        self.write(response)
        return

    if 'token_access' not in data:
        response = {"status":False, "message":"Token Acces Not Found",'data':json.loads(self.request.body)}               
        self.write(response)
        return

    if 'group_id' not in data:
        response = {"status":False, "message":"Group Sensor ID Not Found",'data':json.loads(self.request.body)}               
        self.write(response)
        return

    insert = comChannelController.add(data)    
    if not insert['status']:
        response = {"status":False, "message":"Failed to add", 'data':json.loads(self.request.body)}               
    else:
        response = {'message':'Success','status':True}    
    self.write(response)

class list(RequestHandler):
  def post(self):    
    data = json.loads(self.request.body)
    query = data
    result = comChannelController.find(query)
    if not result['status']:
        response = {"status":False, "message":"Data Not Found",'data':json.loads(self.request.body)}               
    else:
        response = {"status":True, 'message':'Success','data':result['data']}
    self.write(response)

class detail(RequestHandler):
  def post(self):    
    data = json.loads(self.request.body)
    result = comChannelController.findOne(data)
    # print(result)
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

    result = comChannelController.findOne(query)
    if not result['status']:
        response = {"status":False, "message":"Data Not Found",'data':json.loads(self.request.body)}               
    else:
        update = comChannelController.update(query,data)
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

    result = comChannelController.findOne(query)
    if not result['status']:
        response = {"status":False, "message":"Data Not Found",'data':json.loads(self.request.body)}            
    else:
        delete = comChannelController.delete(query)
        if not delete['status']:
            response = {"status":False, "message":"Failed to delete","data":json.loads(self.request.body)}
        else:
            response = {"status":True, 'message':'Delete Success'}
    self.write(response)


def generateCode():
    code = cloud9Lib.randomStringLower(6)

    #check if exist
    query = {"code_name":code}
    result = comChannelController.findOne(query)
    if result['status']:
        return generateCode()
    else:
        return code

def generateToken(codename):
    code = cloud9Lib.randomStringLower(5)
    code = code+codename+cloud9Lib.randomStringLower(6)
    #check if exist
    query = {"token_access":code}
    result = comChannelController.findOne(query)
    if result['status']:
        return generateToken(codename)
    else:
        return code