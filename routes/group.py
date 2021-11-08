import sys
sys.path.append('../')
from tornado.web import RequestHandler
from bson import ObjectId
import json 
from function import *
from controller import groupController
from slugify import slugify

groups = []

#PRIMARY VARIABLE - DONT DELETE
define_url = [
    ['add/','add'],
    ['','list'],
    ['count/','count'],
    ['detail/','detail'],
    ['edit/','update'],
    ['delete/','delete'],
    ['member/add/','addMember'],
    ['member/get/','getMember'],
    ['member/edit/','updateMember'],
    ['member/delete/','removeMember']
    # ['add/','add'],
    # ['','list'],
    # ['detail','detail'],
    # ['edit/','update'],
    # ['delete/([^/]+)','delete']
]

class add(RequestHandler):
  def post(self):    
    data = json.loads(self.request.body) 
    if 'email' not in data:
        response = {"status":False, "message":"Email Not Found",'data':json.loads(self.request.body)}               
        self.write(response)
        return
    if 'name' not in data:
        response = {"status":False, "message":"Group Name Found",'data':json.loads(self.request.body)}               
        self.write(response)
        return
    if 'add_by' not in data:
        response = {"status":False, "message":"Owner Group Found",'data':json.loads(self.request.body)}               
        self.write(response)
        return

    data['group_code'] = slugify(data['name'])
    result = groupController.findOne({"group_code":data['group_code']})
    if result['status']:        
        response = {"status":False, "message":"Dupplicates group",'data':json.loads(self.request.body)} 
        self.write(response)
        return
    insert = groupController.add(data)    
    if not insert['status']:
        response = {"status":False, "message":"Failed to add", 'data':json.loads(self.request.body)}               
    else:
        response = {'message':'Success','status':True}    
    self.write(response)

class list(RequestHandler):
  def post(self):    
    data = json.loads(self.request.body)
    if 'user_id' in data:
        data['member'] = { '$elemMatch': {"user_id":data['user_id'] } }
        del data['user_id']
    query = data
    result = groupController.find(query)
    print(result)
    print("------------------")
    sys.stdout.flush()
    if not result['status']:
        response = {"status":False, "message":"Data Not Found",'data':json.loads(self.request.body)}               
    else:
        response = {"status":True, 'message':'Success','data':result['data']}
    self.write(response)

class detail(RequestHandler):
  def post(self):    
    data = json.loads(self.request.body)
    query = data
    result = groupController.findOne(query)
    print(result)
    print("------------------")
    sys.stdout.flush()
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

    result = groupController.findOne(query)
    if not result['status']:
        response = {"status":False, "message":"Data Not Found",'data':json.loads(self.request.body)}               
    else:
        update = groupController.update(query,data)
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

    query = {"_id":ObjectId(data["id"])}
    result = groupController.findOne(query)
    if not result['status']:
        response = {"status":False, "message":"Data Not Found",'data':json.loads(self.request.body)}            
    else:
        delete = groupController.delete(query)
        if not delete['status']:
            response = {"status":False, "message":"Failed to delete","data":json.loads(self.request.body)}
        else:
            response = {"status":True, 'message':'Delete Success'}
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

    result = groupController.find(query)
    #sys.stdout.flush()
    if not result['status']:
        response = {"status":True, "message":"Data Not Found",'data':0}               
    else:
        response = {"status":True, 'message':'Success','data':len(result['data'])}
    self.write(response)

class addMember(RequestHandler):
  def post(self):        
    data = json.loads(self.request.body)
    if 'id' not in data:
        response = {"status":False, "message":"Id Not Found",'data':json.loads(self.request.body)}               
        self.write(response)
        return

    if 'user_id' not in data:
        response = {"status":False, "message":"User Not Found",'data':json.loads(self.request.body)}               
        self.write(response)
        return

    try:
        query = {"_id":ObjectId(data["id"])}
    except:
        response = {"status":False, "message":"Wrong id",'data':json.loads(self.request.body)}               
        self.write(response) 
        return

    result = groupController.getItemMember(query,data["user_id"])
    print("-----------------------")
    print(result)
    sys.stdout.flush()
    if result['status']:
        response = {"status":False, "message":"Member exists",'data':json.loads(self.request.body)}
        self.write(response)
        return
    del query["member"]
    result = groupController.findOne(query)
    if not result['status']:
        response = {"status":False, "message":"Data Not Found",'data':json.loads(self.request.body)}               
    else:
        update = groupController.addMember(query,data)
        if not update['status']:
            response = {"status":False, "message":"Failed to add member","data":json.loads(self.request.body)}
        else:
            response = {"status":True, 'message':'Add member success'}    
    self.write(response)

class updateMember(RequestHandler):
  def post(self):        
    data = json.loads(self.request.body)
    if 'id' not in data:
        response = {"status":False, "message":"Id Not Found",'data':json.loads(self.request.body)}               
        self.write(response)
        return

    if 'user_id' not in data:
        response = {"status":False, "message":"User Not Found",'data':json.loads(self.request.body)}               
        self.write(response)
        return

    try:
        query = {"_id":ObjectId(data["id"])}
    except:
        response = {"status":False, "message":"Wrong id",'data':json.loads(self.request.body)}               
        self.write(response) 
        return

    result = groupController.getItemMember(query,data["user_id"])
    if not result['status']:
        response = {"status":False, "message":"Data Not Found",'data':json.loads(self.request.body)}               
    else:
        oldData = {}
        listData = result['data']['member']
        for i in listData: 
            if i['user_id'] == data['user_id']:
                oldData = i
                break; 
        del query['member']
        update = groupController.updateMember(query,oldData,data)
        if not update['status']:
            response = {"status":False, "message":"Failed to add member","data":json.loads(self.request.body)}
        else:
            response = {"status":True, 'message':'Update member success'}
    self.write(response)

class removeMember(RequestHandler):
  def post(self):        
    data = json.loads(self.request.body)
    if 'id' not in data:
        response = {"status":False, "message":"Id Not Found",'data':json.loads(self.request.body)}               
        self.write(response)
        return

    if 'user_id' not in data:
        response = {"status":False, "message":"User Not Found",'data':json.loads(self.request.body)}               
        self.write(response)
        return

    try:
        query = {"_id":ObjectId(data["id"])}
    except:
        response = {"status":False, "message":"Wrong id",'data':json.loads(self.request.body)}               
        self.write(response) 
        return

    result = groupController.findOne(query)
    if not result['status']:
        response = {"status":False, "message":"Data Not Found",'data':json.loads(self.request.body)}               
    else:
        update = groupController.removeMember(query,data)
        if not update['status']:
            response = {"status":False, "message":"Failed to add member","data":json.loads(self.request.body)}
        else:
            response = {"status":True, 'message':'Remove member success'}
    self.write(response)

class getMember(RequestHandler):
  def post(self):        
    data = json.loads(self.request.body)
    if 'id' not in data:
        response = {"status":False, "message":"Id Not Found",'data':json.loads(self.request.body)}               
        self.write(response)
        return

    if 'user_id' not in data:
        response = {"status":False, "message":"User Not Found",'data':json.loads(self.request.body)}               
        self.write(response)
        return

    try:
        query = {"_id":ObjectId(data["id"])}
    except:
        response = {"status":False, "message":"Wrong id",'data':json.loads(self.request.body)}               
        self.write(response) 
        return

    result = groupController.getItemMember(query,data["user_id"])
    if not result['status']:
        response = {"status":False, "message":"Data Not Found",'data':json.loads(self.request.body)}               
    else:
        oldData = {}
        result['data'] = result['data']['member']
        for i in result['data']: 
            if i['user_id'] == data['user_id']:
                oldData = i
                break; 
        response = {"status":True, 'message':'Success','data':oldData}
    self.write(response)