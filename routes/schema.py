import sys
sys.path.append('../')
from tornado.web import RequestHandler
from bson import ObjectId
import json 
from function import *
from controller import schemaController
from controller import schemaDataController
from datetime import datetime
from pytz import timezone
from configparser import ConfigParser
config = ConfigParser()
config.read("config.ini")

from datetime import timedelta
td = timedelta(hours=int(config["SERVER"]["timediff"]))
groups = []
prefix_collection = "schema_data_"


#PRIMARY VARIABLE - DONT DELETE
define_url = [
    ['add/','add'],
    ['','list'],
    ['count/','count'],
    ['detail/','detail'],
    ['edit/','update'],
    ['delete/','delete'],
    ['data/([^/]+)/','getSchemaData'],
    ['data/([^/]+)/count/','countSchemaData'],
    ['data/([^/]+)/detail/','detailSchemaData'],
    ['data/([^/]+)/add/','addSchemaData'],
    ['data/([^/]+)/edit/','updateSchemaData'],
    ['data/([^/]+)/delete/','deleteSchemaData'],
    ['data/([^/]+)/group/','groupSchemaData'],
    ['data/([^/]+)','getSchemaData'],
    ['data/([^/]+)/count','countSchemaData'],
    ['data/([^/]+)/detail','detailSchemaData'],
    ['data/([^/]+)/add','addSchemaData'],
    ['data/([^/]+)/edit','updateSchemaData'],
    ['data/([^/]+)/delete','deleteSchemaData'],
    ['data/([^/]+)/group','groupSchemaData']
]

class add(RequestHandler):
  def post(self):    
    data = json.loads(self.request.body)
    print(data)
    sys.stdout.flush()
    if 'schema_code' not in data:
        data['schema_code'] = generateCode()

    insert = schemaController.add(data)    
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
    result = schemaController.find(query)
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
    result = schemaController.find(query)
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
    result = schemaController.findOne(query)    
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

    if 'schema_code' in data:
        if checkSchemaCode(data['schema_code'],query['_id']):
            response = {"status":False, "message":"Schema Code is exits",'data':json.loads(self.request.body)} 
            self.write(response)
            return

    result = schemaController.findOne(query)
    if not result['status']:
        response = {"status":False, "message":"Data Not Found",'data':json.loads(self.request.body)}               
    else:
        update = schemaController.update(query,data)
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
    
    result = schemaController.findOne(query)
    if not result['status']:
        response = {"status":False, "message":"Data Not Found",'data':json.loads(self.request.body)}            
    else:
        delete = schemaController.delete(query)
        if not delete['status']:
            response = {"status":False, "message":"Failed to delete","data":json.loads(self.request.body)}
        else:
            response = {"status":True, 'message':'Delete Success'}
    self.write(response)

class getSchemaData(RequestHandler):
  def post(self,code):    
    if not code:
        response = {"status":False, "message":"Schema Code not found",'data':json.loads(self.request.body)}               
        self.write(response)
        return
    data = json.loads(self.request.body)
    print(data)
    response = ""
    query = {"schema_code":code}
    schemaData = schemaController.findOne(query)
    if not schemaData['status']:
        response = {"status":False, "message":"Schema Not Found",'data':json.loads(self.request.body)}               
    else:
        schemaData = schemaData['data']
        if response == "":
            limit =  None
            skip = None
            showid = None
            sort = ('date_add_auto',-1)
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
                data['date_add_auto'] = {"$gte":datesrc_str, "$lt":datesrc_end }
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
                data['date_add_auto'] = {"$gte":datesrc_str, "$lt":datesrc_end }
                del data['date_start']
                del data['date_end']
            if 'showid' in data:
                showid = data['showid']
                del data['showid']


            query = data
            exclude = None
            print(query)
            result = schemaDataController.find(prefix_collection+code,query,exclude,limit,skip,sort,showid)
            if not result['status']:
                response = {"status":False, "message":"Data Not Found",'data':json.loads(self.request.body)}               
            else:
                response = {"status":True, 'message':'Success','data':result['data']}
    self.write(response)

class countSchemaData(RequestHandler):
  def post(self,code):    
    data = json.loads(self.request.body)

    if not code:
        response = {"status":False, "message":"Schema Code not found",'data':json.loads(self.request.body)}               
        self.write(response)
        return

    print(data)
    response = ""
    query = {"schema_code":code}
    schemaData = schemaController.findOne(query)
    if not schemaData['status']:
        response = {"status":False, "message":"Schema Not Found",'data':json.loads(self.request.body)}               
    else:
        schemaData = schemaData['data']
        if response == "":
            limit =  None
            skip = None
            sort = ('date_add_auto',-1)
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
                datesrc_end = datetime.strptime(date_time_str+" 23:59:59",'%Y-%m-%d %H:%M:%S') - td
                data['date_add_auto'] = {"$gte":datesrc_str, "$lt":datesrc_end }
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
                data['date_add_auto'] = {"$gte":datesrc_str, "$lt":datesrc_end }
                del data['date_start']
                del data['date_end']
            query = data
            exclude = None
            result = schemaDataController.find(prefix_collection+code,query,exclude,limit,skip,sort)
            if not result['status']:
                response = {"status":False, "message":"Data Not Found",'data':json.loads(self.request.body)}               
            else:
                response = {"status":True, 'message':'Success','data':len(result['data'])}
    self.write(response)

class detailSchemaData(RequestHandler):
  def post(self,schema_code):    
    data = json.loads(self.request.body)    
    if not schema_code:
        response = {"status":False, "message":"Schema Code not found",'data':json.loads(self.request.body)}               
        self.write(response)
        return
    query = {"schema_code":schema_code}
    schemaData = schemaController.findOne(query)
    if not schemaData['status']:
        response = {"status":False, "message":"Schema Not Found",'data':json.loads(self.request.body)}      
        self.write(response)
        return             
    if "_id" in data :
        try:
            data["_id"] = ObjectId(data["_id"])
        except:
            data["_id"] = data["_id"]   
    # elif "id" in query :
    #     try:
    #         query["_id"] = ObjectId(query["id"])
    #         del query["id"]
    #     except:
    #         del query["id"]
    sort = ('date_add_auto',-1)
    exclude = None
    showid = None
    if 'sort' in data:
        sort = (data['sort']['field'],data['sort']['type'])            
        del data['sort']
    if 'showid' in data:
        showid = data['showid']
        del data['showid']
    query = data
    result = schemaDataController.findOne(prefix_collection+schema_code,query,exclude,sort,showid)    
    if not result['status']:
        response = {"status":False, "message":"Data Not Found",'data':json.loads(self.request.body)}               
    else:
        response = {"status":True, 'message':'Success','data':result['data']}
    self.write(response)

class addSchemaData(RequestHandler):
  def post(self,schema_code):    
    data = json.loads(self.request.body)
    if not schema_code:
        response = {"status":False, "message":"Schema Code not found",'data':json.loads(self.request.body)}               
        self.write(response)
        return
    # filter = schemaDataController.filterAdd(schema_code,data)
    # if filter['status']:
    #     response = {'message':'Success','status':True} 
    # else:
    #     response = {"status":False, "message":"Failed to add", 'data':json.loads(self.request.body)}  
    filter = schemaDataController.filter(schema_code,data)
    if filter['status']:
        data = filter['data']
        data["date_add_auto"] = datetime.now(timezone('Asia/Tokyo'))
        insert = schemaDataController.add(prefix_collection+schema_code,data)
        if insert['status']:
            response = {'status':True, 'message':"Success"}
        else:
            response = {"status":False, "message":"Failed to add", 'data':json.loads(self.request.body)}
    else:
        response = {"status":False, "message":"Failed to add", 'data':json.loads(self.request.body)}
    self.write(response)

class updateSchemaData(RequestHandler):
  def post(self,schema_code):    
    data = json.loads(self.request.body)
    if not schema_code:
        response = {"status":False, "message":"Schema Code not found",'data':json.loads(self.request.body)}               
        self.write(response)
        return
    
    if '_id' not in data:
        response = {"status":False, "message":"Id Not Found",'data':json.loads(self.request.body)}               
        self.write(response)
        return
    
    try:
        query = {"_id":ObjectId(data["_id"])}
        del data["_id"]
        # elif "id" in data :
        #     query = {"_id":ObjectId(data["id"])}
        #     del data["id"]
    except:
        response = {"status":False, "message":"Wrong id",'data':json.loads(self.request.body)}               
        self.write(response) 
        return

    result = schemaDataController.findOne(prefix_collection+schema_code,query)
    if not result['status']:
        response = {"status":False, "message":"Data Not Found",'data':json.loads(self.request.body)}               
    else:
        filter = schemaDataController.filter(schema_code,data)
        if filter['status']:
            data = filter['data']
            update = schemaDataController.update(prefix_collection+schema_code,query,data)
            if update['status']:
                response = {'status':True, 'message':"Success"}
            else:
                response = {"status":False, "message":"Failed to update", 'data':json.loads(self.request.body)}
        else:
            response = {"status":False, "message":"Failed to update", 'data':json.loads(self.request.body)}
    self.write(response)

class deleteSchemaData(RequestHandler):
  def post(self,schema_code):        
    data = json.loads(self.request.body)
    if not schema_code:
        response = {"status":False, "message":"Schema Code not found",'data':json.loads(self.request.body)}               
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
    
    result = schemaDataController.findOne(prefix_collection+schema_code,query)
    if not result['status']:
        response = {"status":False, "message":"Data Not Found",'data':json.loads(self.request.body)}            
    else:
        delete = schemaDataController.delete(prefix_collection+schema_code,query)
        if not delete['status']:
            response = {"status":False, "message":"Failed to delete","data":json.loads(self.request.body)}
        else:
            response = {"status":True, 'message':'Delete Success'}
    self.write(response)

class groupSchemaData(RequestHandler):
  def post(self,code):    
    if not code:
        response = {"status":False, "message":"Schema Code not found",'data':json.loads(self.request.body)}               
        self.write(response)
        return
    data = json.loads(self.request.body)
    print(data)
    response = ""
    query = {"schema_code":code}
    schemaData = schemaController.findOne(query)
    if not schemaData['status']:
        response = {"status":False, "message":"Schema Not Found",'data':json.loads(self.request.body)}               
    else:
        schemaData = schemaData['data']
        if response == "":
            query = {}
            limit =  None
            skip = None
            groupby = "_id"
            if 'groupby' in data:
                groupby = data["groupby"]
            if 'query' in data:
                query = data['query']
                if 'date' in query:
                    date_time_str = str(data['date'])
                    datesrc_str = datetime.strptime(date_time_str+" 00:00:00",'%Y-%m-%d %H:%M:%S') - td
                    datesrc_end = datetime.strptime(date_time_end+" 23:59:59",'%Y-%m-%d %H:%M:%S') - td
                    query['date_add_auto'] = {"$gte":datesrc_str, "$lt":datesrc_end }                
                if 'date_start' in query and 'date_end' in query:
                    date_time_str = str(data['date_start'])
                    date_time_end = str(data['date_end'])
                    datesrc_str = datetime.strptime(date_time_str+" 00:00:00",'%Y-%m-%d %H:%M:%S') - td
                    datesrc_end = datetime.strptime(date_time_end+" 23:59:59",'%Y-%m-%d %H:%M:%S') - td
                    query['date_add_auto'] = {"$gte":datesrc_str, "$lt":datesrc_end }
                    del query['date_start']
                    del query['date_end']
            method = "last"
            if "method" in data:
                method = data["method"]
            
            if method == "last":
                group = {
                    "_id":"$"+groupby,
                    "date":{"$last":"$date_add_auto"}
                }
                if 'field' in data:
                    for item in data["field"]:
                        group[item] = {"$last":"$"+data["field"][item]}    
            pipeline = [
                {"$match":query},
                {"$group":group}
            ]
            print(pipeline)
            result = schemaDataController.aggregate(prefix_collection+code,pipeline)
            if not result['status']:
                response = {"status":False, "message":"Data Not Found",'data':json.loads(self.request.body)}               
            else:
                response = {"status":True, 'message':'Success','data':result['data']}
    self.write(response)

def generateCode(code=""):
    if code == "":
        code = cloud9Lib.randomStringLower(6)
    else:
        code = code+"-"+cloud9Lib.randomStringLower(6)
    #check if exist
    query = {"schema_code":code}
    result = schemaController.findOne(query)
    if result['status']:
        return generateCode(code)
    else:
        return code

def checkSchemaCode(code,execpt=""):
    if execpt:
        query = {"schema_code":code,"_id":{ '$ne' : execpt } }
        result = schemaController.findOne(query)
    else:
        query = {"schema_code":code}
        result = schemaController.findOne(query)
    print(result)
    if result['status']:
        return True
    else:
        return False