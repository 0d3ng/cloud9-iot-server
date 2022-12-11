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
from datetime import datetime
from pytz import timezone


from configparser import ConfigParser
config = ConfigParser()
config.read("config.ini")

from datetime import timedelta
td = timedelta(hours=int(config["SERVER"]["timediff"]))
mqtt_device = "edge/update/"

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
    ['data/([^/]+)/count/','countdata'],
    ['add/other/','addOther'],
    ['edit/other/','updateOther'],
    ['delete/other/','deleteOther'],
    ['edge/add/','addEdge'],
    ['edge/list/','listEdge'],
    ['edge/count/','countEdge'],
    ['edge/detail/','detailEdge'],
    ['edge/edit/','updateEdge'],
    ['edge/delete/','deleteEdge'],
    ['edge/config/process/','configEdge'],
    ['edge/device/init','deviceInitEdge'],
    ['edge/device/config/','deviceGetEdgeConfig'],
    ['edge/device/update/','deviceUpdateEdge']
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
        data['device_code'] = generateCode(data['group_code_name'])

    insert = deviceController.add(data)    
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
    result = deviceController.find(query)
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
    if "id" in query :
        try:
            query["_id"] = ObjectId(query["id"])
            del query["id"]
        except:
            del query["id"]
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
                collection = 'sensor_data_'+groupData['id']
        else:
            collection = 'sensor_data_'+deviceData['device_code']
        
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
            print(query)
            result = sensorController.find(collection,query,exclude,limit,skip,sort)
            if not result['status']:
                response = {"status":False, "message":"Data Not Found",'data':json.loads(self.request.body)}               
            else:
                response = {"status":True, 'message':'Success','data':result['data']}
    self.write(response)

class countdata(RequestHandler):
  def post(self,device):    
    data = json.loads(self.request.body)
    print(data)
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
                collection = 'sensor_data_'+groupData['id']
        else:
            collection = 'sensor_data_'+deviceData['device_code']
        
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
            print(query)
            result = sensorController.count(collection,query,exclude,limit,skip,sort)
            if not result['status']:
                response = {"status":False, "message":"Data Not Found",'data':0}               
            else:
                response = {"status":True, 'message':'Success','data':result['data']}
    self.write(response)

class addOther(RequestHandler):
  def post(self):    
    data = json.loads(self.request.body)
    print(data)
    sys.stdout.flush()

    if 'key_access' not in data:
        data['key_access'] = generateAccess()
    else:
        if checkKeyAccess(data['key_access']):
            response = {"status":False, "message":"Key access is exits",'data':json.loads(self.request.body)} 
            self.write(response)
            return
            
    if 'device_code' not in data:
        data['device_code'] = generateCode()

    if 'group_code_name' not in data:
        data['group_code_name'] = "other"

    insert = deviceController.addOther(data)    
    if not insert['status']:
        response = {"status":False, "message":"Failed to add", 'data':json.loads(self.request.body)}               
    else:
        response = {'message':'Success','status':True}    
    self.write(response)

class updateOther(RequestHandler):
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
        update = deviceController.updateOther(query,data)
        if not update['status']:
            response = {"status":False, "message":"Failed to update","data":json.loads(self.request.body)}
        else:
            response = {"status":True, 'message':'Update Success'}
    self.write(response)

class deleteOther(RequestHandler):
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
        delete = deviceController.deleteOther(query,result['data'])
        if not delete['status']:
            response = {"status":False, "message":"Failed to delete","data":json.loads(self.request.body)}
        else:
            response = {"status":True, 'message':'Delete Success'}
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

def generateCode(code=""):
    if code == "":
        code = cloud9Lib.randomOnlyString(2)+cloud9Lib.randomNumber(2)
    else:
        code = code+"-"+cloud9Lib.randomOnlyString(2)+cloud9Lib.randomNumber(2)
    #check if exist
    query = {"device_code":code}
    result = deviceController.findOne(query)
    if result['status']:
        return generateCode(code)
    else:
        return code

def generateEdgeCode(code=""):
    if code == "":
        code = cloud9Lib.randomOnlyString(2)+cloud9Lib.randomNumber(2)
    else:
        code = code+"-"+cloud9Lib.randomOnlyString(2)+cloud9Lib.randomNumber(2)
    #check if exist
    query = {"edgeconfig_code":code}
    result = edgeController.findOne(query)
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

#--------------------------------
class addEdge(RequestHandler):
  def post(self):    
    data = json.loads(self.request.body)        
    if 'device_code' not in data:
        response = {"status":False, "message":"Parameter device_code not exists",'data':json.loads(self.request.body)}               
        self.write(response)
        return
    #check if exist
    query = {"device_code":data['device_code']}
    result = deviceController.findOne(query)
    if not result['status']:
        response = {"status":False, "message":"Device not found",'data':json.loads(self.request.body)}               
        self.write(response)
        return

    if 'edgeconfig_code' not in data:
        data['edgeconfig_code'] = generateCode()

    insert = edgeController.add(data)    
    if not insert['status']:
        response = {"status":False, "message":"Failed to add", 'data':json.loads(self.request.body)}               
    else:
        response = {'message':'Success','status':True, 'data':{"id":insert['data']}}    
    self.write(response)


class listEdge(RequestHandler):
  def post(self):    
    data = json.loads(self.request.body)    
    query = data    
    result = edgeController.find(query)
    if not result['status']:
        response = {"status":False, "message":"Data Not Found",'data':json.loads(self.request.body)}               
    else:
        response = {"status":True, 'message':'Success','data':result['data']}
    self.write(response)

class countEdge(RequestHandler):
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
    result = edgeController.find(query)
    if not result['status']:
        response = {"status":False, "message":"Data Not Found",'data':0}               
    else:
        response = {"status":True, 'message':'Success','data':len(result['data'])}
    self.write(response)

class detailEdge(RequestHandler):
  def post(self):    
    data = json.loads(self.request.body)
    query = data    
    if "id" in query :
        try:
            query["_id"] = ObjectId(query["id"])
            del query["id"]
        except:
            del query["id"]
    result = edgeController.findOne(query)       
    if not result['status']:
        response = {"status":False, "message":"Data Not Found",'data':json.loads(self.request.body)}               
    else:
        response = {"status":True, 'message':'Success','data':result['data']}
    self.write(response)

class updateEdge(RequestHandler):
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

    result = edgeController.findOne(query)
    if not result['status']:
        response = {"status":False, "message":"Data Not Found",'data':json.loads(self.request.body)}               
    else:
        update = edgeController.update(query,data)
        if not update['status']:
            response = {"status":False, "message":"Failed to update","data":json.loads(self.request.body)}
        else:
            response = {"status":True, 'message':'Update Success'}
    self.write(response)

class deleteEdge(RequestHandler):
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
    
    result = edgeController.findOne(query)
    if not result['status']:
        response = {"status":False, "message":"Data Not Found",'data':json.loads(self.request.body)}            
    else:
        delete = edgeController.delete(query)
        if not delete['status']:
            response = {"status":False, "message":"Failed to delete","data":json.loads(self.request.body)}
        else:
            response = {"status":True, 'message':'Delete Success'}
    self.write(response)


class configEdge(RequestHandler):
  def post(self):  
    result = {}  
    data = json.loads(self.request.body)
    if 'method' not in data:
        response = {"status":False, "message":"Method Not Found",'data':json.loads(self.request.body)}               
        self.write(response)
        return  
    if 'string_sample' not in data:
        response = {"status":False, "message":"String Sample Not Found",'data':json.loads(self.request.body)}               
        self.write(response)
        return    
    delimeter = [None,None]
    method = data["method"]
    string_sample = data["string_sample"]
    if 'delimeter' in data:
        delimeter = data["delimeter"]
    result['data'] = data.copy()
    if method == "array_list" :
        res_list,delim1 = edgeController.covert_to_list(string_sample,delimeter[0])
        if res_list == False :
            result['status'] = False
        else:
            result['status'] = True            
            result['data']['delimeter'] = [delim1]
            result['data']['list'] = res_list
            string_pattern = ""
            i = 0
            for item in res_list:
                string_pattern+="item["+str(i)+"]"                                
                i+=1
                if(len(res_list)>i):
                    string_pattern+=str(delim1)

            result['data']["string_pattern"] = string_pattern
    elif method == "json_object" :
        res_object,delim1,delim2 = edgeController.convert_to_json(string_sample,delimeter[0],delimeter[1])
        if res_object == False :
            result['status'] = False
        else:
            result['status'] = True  
            result['data']['delimeter'] = [delim1,delim2]
            result['data']['object'] = res_object        
            string_pattern = ""   
            i = 0         
            for item in res_object:
                string_pattern+=item+delim2+"["+str(item)+"-value]"    
                i+=1                                            
                if(len(res_object)>i):
                    string_pattern+=str(delim1)
            result['data']["string_pattern"] = string_pattern
    else:
        result['status'] = False

    if not result['status']:
        response = {"status":False, "message":"Process Failed",'data':json.loads(self.request.body)}               
    else:
        response = {"status":True, 'message':'Success','data':result['data']}
    self.write(response)


class deviceInitEdge(RequestHandler):
  def post(self):        
    data = json.loads(self.request.body)
    ##CEK username, pass, device id, edge_device_id
    if 'email' not in data:
        response = {"status":False, "message":"Email Not Found",'data':json.loads(self.request.body)}               
        self.write(response)
        return

    if 'password' not in data:
        response = {"status":False, "message":"Password Not Found",'data':json.loads(self.request.body)}               
        self.write(response)
        return

    if 'device_code' not in data:
        response = {"status":False, "message":"Device Code Not Found",'data':json.loads(self.request.body)}               
        self.write(response)
        return
    
    if 'edge_device_id' not in data:
        response = {"status":False, "message":"Edge Device ID Not Found",'data':json.loads(self.request.body)}               
        self.write(response)
        return
    
    if 'device_info' not in data: 
        device_info = {}
    else:
        device_info = data["device_info"]


    query = {'email':data['email']}
    result = userController.findOne(query)
    if not result['status']:
        account = False               
    else:
        password_db = cloud9Lib.decrypt(result['data']['password'])
        password = data['password']
        if password == password_db:
            if result['data']['active'] == False:
                account = False
            else :
                account = True
        else:
            account = False
    
    if account == False:
        response = {"status":False, "message":"Account not Active",'data':json.loads(self.request.body)}               
        self.write(response)
        return
    
    query = {
        'device_code':data["device_code"],
        'active' : True
    }
    edge_data = edgeController.findOne(query)
    if edge_data["status"] == False:
        response = {"status":False, "message":"Edge Configuration not Found",'data':json.loads(self.request.body)}               
        self.write(response)
        return
    edge_data = edge_data["data"]
    edge_config = edgeController.config_file(edge_data["edgeconfig_code"],config)
    response = {"status":True, 'message':'Success','data':edge_config['data']}
    query = {
        "edgeconfig_code" : edge_data["edgeconfig_code"]
    }
    updatedata = {
        "date_download" : datetime.now(timezone('Asia/Tokyo')).strftime('%Y-%m-%d %H:%M:%S')
    }
    update = edgeController.update(query,updatedata)
    query = {
        "device_code" : data["device_code"]
    }
    updatedata = {
        "connected_device" : {
            "id": data["edge_device_id"],
            "device_info":device_info
        }
    }
    update = deviceController.update(query,updatedata)

    self.write(response)
    

class deviceUpdateEdge(RequestHandler):
  def post(self):        
    data = json.loads(self.request.body)
    if 'edgeconfig_code' not in data:
        response = {"status":False, "message":"Edge Device Code Not Found",'data':json.loads(self.request.body)}               
        self.write(response)
        return

    query = {
        'edgeconfig_code':data["edgeconfig_code"],
        'active' : True
    }
    edge_data = edgeController.findOne(query)
    if edge_data["status"] == False:
        response = {"status":False, "message":"Edge Configuration not Found",'data':json.loads(self.request.body)}               
        self.write(response)
        return        
    edge_data = edge_data['data']
    query = {"device_code":edge_data['device_code']}
    device_data = deviceController.findOne(query)
    if not device_data['status']:
        response = {"status":False, "message":"Device not found",'data':json.loads(self.request.body)}               
        self.write(response)
        return
    device_data = device_data["data"]
    if("connected_device" in device_data):
        if("id" in device_data["connected_device"]):
            edge_config = edgeController.config_file(data["edgeconfig_code"],config)
            edge_config = edge_config['data']
            mqttcom.publish(mqtt_device+device_data["connected_device"]["id"],edge_config)
            query = {
                "edgeconfig_code" : edge_data["edgeconfig_code"]
            }
            updatedata = {
                "date_download" : datetime.now(timezone('Asia/Tokyo')).strftime('%Y-%m-%d %H:%M:%S')
            }
            update = edgeController.update(query,updatedata)  
            response = {"status":True, 'message':'Success'}
        else:
           response = {"status":False, "message":"Device not found",'data':json.loads(self.request.body)}  
    else:
        response = {"status":False, "message":"Device not found",'data':json.loads(self.request.body)}  

    self.write(response)
    return
    

class deviceGetEdgeConfig(RequestHandler):
  def post(self):        
    data = json.loads(self.request.body)
    if 'edgeconfig_code' not in data:
        response = {"status":False, "message":"Edge Device Code Not Found",'data':json.loads(self.request.body)}               
        self.write(response)
        return

    query = {
        'edgeconfig_code':data["edgeconfig_code"],
        'active' : True
    }
    edge_data = edgeController.findOne(query)
    if edge_data["status"] == False:
        response = {"status":False, "message":"Edge Configuration not Found",'data':json.loads(self.request.body)}               
        self.write(response)
        return        
    edge_data = edge_data['data']
    query = {"device_code":edge_data['device_code']}
    device_data = deviceController.findOne(query)
    if not device_data['status']:
        response = {"status":False, "message":"Device not found",'data':json.loads(self.request.body)}               
        self.write(response)
        return
    edge_config = edgeController.config_file(data["edgeconfig_code"],config)
    if edge_config['status']:
        response = {"status":True, 'message':'Success', 'data':edge_config['data']}
    else:
        response = {"status":False, "message":"Configuration not found",'data':json.loads(self.request.body)} 

    self.write(response)
    return