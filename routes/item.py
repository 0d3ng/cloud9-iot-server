import sys
sys.path.append('../')
from tornado.web import RequestHandler
from bson import ObjectId
import json 
from function import *

items = []

define_url = [
	['add/','add'],
	['','list'],
	['edit/','update'],
	['delete/([^/]+)','delete']
]



class add(RequestHandler):
  def post(self):    
    data = json.loads(self.request.body)
    if db.insertData("item",data) == []:
        response = {"status":False, "message":json}               
    else:
        response = {'message':'Success','status':True}
    self.write(response)

class list(RequestHandler):
  def post(self):    
    data = json.loads(self.request.body)
    query = {}
    result = db.getData("item",query)
    if result == []:
        response = {"status":False, "message":data}               
    else:
        response = {'message':'Success','list':result}
    self.write(response)

class update(RequestHandler):
  def post(self):    
    data = json.loads(self.request.body)
    query = {"_id":ObjectId(data["id"])}
    result = db.getData("item",query)
    if result == []:
        response = {"status":False, "message":data}               
    else:
        if not db.updateData("item",query,data['value']):
            response = {"status":False, "message":data}
        else:
            response = {'message':'Success'}
    self.write(response)

class delete(RequestHandler):
  def post(self,id):    
    print(id)
    query = {"_id":ObjectId(id)}
    result = db.getData("item",query)
    if result == []:
        response = {"status":False, "message":data}               
    else:
        if not db.deleteData("item",query):
            response = {"status":False, "message":data}
        else:
            response = {'message':'Success'}
    self.write(response)