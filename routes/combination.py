import sys
from numpy.lib.function_base import insert
sys.path.append('../')
from tornado.web import RequestHandler
from bson import ObjectId
import json 
from function import *
from controller import combiController
from datetime import datetime,timedelta
from pytz import timezone

groups = []

#PRIMARY VARIABLE - DONT DELETE
define_url = [
    ['add/','add'],
    ['','list'],
    ['count/','count'],
    ['detail/','detail'],
    ['edit/','update'],
    ['delete/','delete'],
    ['batch/([^/]+)/','batch']
]

class add(RequestHandler):
  def post(self):    
    data = json.loads(self.request.body)
    print(data)
    sys.stdout.flush()
    if 'combi_code' not in data:
        data['combi_code'] = generateCode()

    insert = combiController.add(data)    
    if not insert['status']:
        response = {"status":False, "message":"Failed to add", 'data':json.loads(self.request.body)}               
    else:
        response = {'message':'Success','status':True}    
    self.write(response)

class list(RequestHandler):
  def post(self):    
    data = json.loads(self.request.body)
    if 'name' in data:
        data['name'] = {"$regex": data['name']}
    if 'detail' in data:
        data['information.detail'] = {"$regex": data['detail']}
        del data['detail']
    if 'purpose' in data:
        data['information.purpose'] = {"$regex": data['purpose']}
        del data['purpose']
    query = data    
    result = combiController.find(query)
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
    query = data
    result = combiController.find(query)
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
    result = combiController.findOne(query)    
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

    if 'combi_code' in data:
        if checkCombiCode(data['combi_code'],query['_id']):
            response = {"status":False, "message":"Combination Code is exits",'data':json.loads(self.request.body)} 
            self.write(response)
            return

    result = combiController.findOne(query)
    if not result['status']:
        response = {"status":False, "message":"Data Not Found",'data':json.loads(self.request.body)}               
    else:
        update = combiController.update(query,data)
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
    
    result = combiController.findOne(query)
    if not result['status']:
        response = {"status":False, "message":"Data Not Found",'data':json.loads(self.request.body)}            
    else:
        delete = combiController.delete(query)
        if not delete['status']:
            response = {"status":False, "message":"Failed to delete","data":json.loads(self.request.body)}
        else:
            response = {"status":True, 'message':'Delete Success'}
    self.write(response)

class batch(RequestHandler):
  def post(self,combiCode):
    data = json.loads(self.request.body)
    query = {"combi_code":combiCode}
    combiData = combiController.findOne(query)
    if not combiData['status']:
        response = {"status":False, "message":"Combination Function Not Found",'data':json.loads(self.request.body)}               
    else:
        combiData = combiData["data"]
        if 'date_start' not in data:
            response = {"status":False, "message":"Date Start Not Found",'data':json.loads(self.request.body)}               
            self.write(response)
            return
        if 'date_end' not in data:
            response = {"status":False, "message":"Date End Not Found",'data':json.loads(self.request.body)}               
            self.write(response)
            return
        if 'batch_code' not in data:
            batch_code = generateCode("batch")
        listTime = combiController.generateDate(data["date_start"],data["date_end"],combiData["time_loop"])
        insertCount = 0
        for x in range(len(listTime)-1):
            time_start = listTime[x]
            time_end = listTime[x+1]
            time_end = datetime.strptime(time_end,'%Y-%m-%d %H:%M') - timedelta(minutes=1)
            time_end = time_end.strftime('%Y-%m-%d %H:%M')
            insertCount = insertCount + combiController.combiProcess(combiData["schema_code"],combiData["field"],time_start,time_end,batch_code)
            
        response = {"status":True,"data":{"insert_count":insertCount}}
    self.write(response)        
    

def generateCode(code=""):
    if code == "":
        code = cloud9Lib.randomStringLower(6)
    else:
        code = code+"-"+cloud9Lib.randomStringLower(6)
    #check if exist
    query = {"combi_code":code}
    result = combiController.findOne(query)
    if result['status']:
        return generateCode(code)
    else:
        return code

def checkCombiCode(code,execpt=""):
    if execpt:
        query = {"combi_code":code,"_id":{ '$ne' : execpt } }
        result = combiController.findOne(query)
    else:
        query = {"combi_code":code}
        result = combiController.findOne(query)
    print(result)
    if result['status']:
        return True
    else:
        return False
