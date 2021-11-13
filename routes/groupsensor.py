import sys
sys.path.append('../')
from tornado.web import RequestHandler
from bson import ObjectId
import json 
from function import *
from controller import groupSensorController

groups = []

#PRIMARY VARIABLE - DONT DELETE
define_url = [
    ['add/','add'],
    ['','list'],
    ['count/','count'],
    ['detail/','detail'],
    ['edit/','update'],
    ['delete/','delete']
]

class add(RequestHandler):
  def post(self):    
    data = json.loads(self.request.body)
    if 'code_name' not in data:
        data['code_name'] = generateCode()

    if 'token_access' not in data:
        data['token_access'] = generateToken(data['code_name'])

    insert = groupSensorController.add(data)    
    if not insert['status']:
        response = {"status":False, "message":"Failed to add", 'data':json.loads(self.request.body)}               
    else:
        response = {'message':'Success','status':True,'data':insert['data']}    
    self.write(response)

class list(RequestHandler):
  def post(self):    
    data = json.loads(self.request.body)
    if 'name' in data:
        data['name'] = {"$regex": data['name']}
    if 'location' in data:
        data['information.location'] = {"$regex": data['location']}
        del data['location']
    if 'detail' in data:
        data['information.detail'] = {"$regex": data['detail']}
        del data['detail']
    if 'purpose' in data:
        data['information.purpose'] = {"$regex": data['purpose']}
        del data['purpose']
    query = data
    if "id" in query :
        try:
            query["_id"] = ObjectId(query["id"])
            del query["id"]
        except:
            del query["id"]
    result = groupSensorController.find(query)
    if not result['status']:
        response = {"status":False, "message":"Data Not Found",'data':json.loads(self.request.body)}               
    else:
        response = {"status":True, 'message':'Success','data':result['data']}
    self.write(response)

class count(RequestHandler):
  def post(self):    
    data = json.loads(self.request.body)
    if 'name' in data:
        data['name'] = {"$regex": data['name']}
    if 'location' in data:
        data['information.location'] = {"$regex": data['location']}
        del data['location']
    if 'detail' in data:
        data['information.detail'] = {"$regex": data['detail']}
        del data['detail']
    if 'purpose' in data:
        data['information.purpose'] = {"$regex": data['purpose']}
        del data['purpose']
    query = data
    if "id" in query :
        try:
            query["_id"] = ObjectId(query["id"])
            del query["id"]
        except:
            del query["id"]
    result = groupSensorController.find(query)
    if not result['status']:
        response = {"status":False, "message":"Data Not Found",'data':0}               
    else:
        response = {"status":True, 'message':'Success','data':len(result['data'])}
    self.write(response)

class detail(RequestHandler):
  def post(self):    
    data = json.loads(self.request.body)
    query = data
    if "id" in query :
        try:
            query["_id"] = ObjectId(query["id"])
            del query["id"]
        except:
            del query["id"]
    result = groupSensorController.findOne(query)
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

    result = groupSensorController.findOne(query)
    if not result['status']:
        response = {"status":False, "message":"Data Not Found",'data':json.loads(self.request.body)}               
    else:
        update = groupSensorController.update(query,data)
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

    result = groupSensorController.findOne(query)
    if not result['status']:
        response = {"status":False, "message":"Data Not Found",'data':json.loads(self.request.body)}            
    else:
        delete = groupSensorController.delete(query)
        if not delete['status']:
            response = {"status":False, "message":"Failed to delete","data":json.loads(self.request.body)}
        else:
            response = {"status":True, 'message':'Delete Success'}
    self.write(response)


def generateCode():
    code = cloud9Lib.randomStringLower(6)

    #check if exist
    query = {"code_name":code}
    result = groupSensorController.findOne(query)
    if result['status']:
        return generateCode()
    else:
        return code

def generateToken(codename):
    code = cloud9Lib.randomStringLower(5)
    code = code+codename+cloud9Lib.randomStringLower(6)
    #check if exist
    query = {"token_access":code}
    result = groupSensorController.findOne(query)
    if result['status']:
        return generateToken(codename)
    else:
        return code