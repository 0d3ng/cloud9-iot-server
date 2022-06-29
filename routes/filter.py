import sys
sys.path.append('../')
from tornado.web import RequestHandler
from bson import ObjectId
import json 
from function import *
from controller import deviceController
from controller import groupSensorController
from controller import sensorController
from controller import filterController
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
    ['','list'],
    ['count/','count'],
    ['detail/','detail'],
    ['edit/','update'],
    ['delete/','delete'],
    ['batch/([^/]+)/','batch'],
    ['simulation/([^/]+)/','simulation'],
    ['summary/([^/]+)/','summary']
]

class add(RequestHandler):
  def post(self):    
    data = json.loads(self.request.body)
    print(data)
    sys.stdout.flush()
    if 'filter_code' not in data:
        data['filter_code'] = generateCode()

    insert = filterController.add(data)    
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
    result = filterController.find(query)
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
    result = filterController.find(query)
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
    result = filterController.findOne(query)    
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

    if 'filter_code' in data:
        if checkCombiCode(data['filter_code'],query['_id']):
            response = {"status":False, "message":"Data Filter Code is exits",'data':json.loads(self.request.body)} 
            self.write(response)
            return

    result = filterController.findOne(query)
    if not result['status']:
        response = {"status":False, "message":"Data Not Found",'data':json.loads(self.request.body)}               
    else:
        update = filterController.update(query,data)
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
    
    result = filterController.findOne(query)
    if not result['status']:
        response = {"status":False, "message":"Data Not Found",'data':json.loads(self.request.body)}            
    else:
        delete = filterController.delete(query)
        if not delete['status']:
            response = {"status":False, "message":"Failed to delete","data":json.loads(self.request.body)}
        else:
            response = {"status":True, 'message':'Delete Success'}
    self.write(response)

class batch(RequestHandler):
  def post(self,filterCode):
    data = json.loads(self.request.body)
    query = {"filter_code":filterCode}
    filterData = filterController.findOne(query)
    if not filterData['status']:
        response = {"status":False, "message":"Data Filter Function Not Found",'data':json.loads(self.request.body)}               
    else:
        # filterData = filterData["data"]
        # if 'date_start' not in data:
        #     response = {"status":False, "message":"Date Start Not Found",'data':json.loads(self.request.body)}               
        #     self.write(response)
        #     return
        # if 'date_end' not in data:
        #     response = {"status":False, "message":"Date End Not Found",'data':json.loads(self.request.body)}               
        #     self.write(response)
        #     return
        # if 'batch_code' not in data:
        #     batch_code = generateCode("batch")
        # listTime = filterController.generateDate(data["date_start"],data["date_end"],filterData["time_loop"])
        insertCount = 0
        # for x in range(len(listTime)-1):
        #     time_start = listTime[x]
        #     time_end = listTime[x+1]
        #     # time_end = datetime.strptime(time_end,'%Y-%m-%d %H:%M:%S')
        #     # time_end = time_end.strftime('%Y-%m-%d %H:%M')
        #     insertCount = insertCount + filterController.filterProcess(filterData["schema_code"],filterData["field"],time_start,time_end,batch_code)
            
        response = {"status":True,"data":{"insert_count":insertCount}}
    self.write(response)        
    

def generateCode(code=""):
    if code == "":
        code = cloud9Lib.randomStringLower(6)
    else:
        code = code+"-"+cloud9Lib.randomStringLower(6)
    #check if exist
    query = {"filter_code":code}
    result = filterController.findOne(query)
    if result['status']:
        return generateCode(code)
    else:
        return code

def checkCombiCode(code,execpt=""):
    if execpt:
        query = {"filter_code":code,"_id":{ '$ne' : execpt } }
        result = filterController.findOne(query)
    else:
        query = {"filter_code":code}
        result = filterController.findOne(query)
    print(result)
    if result['status']:
        return True
    else:
        return False


class simulation(RequestHandler):
  def post(self,device):    
    data = json.loads(self.request.body)
    response = ""
    query = {"device_code":device}
    deviceData = deviceController.findOne(query)
    if not deviceData['status']:
        response = {"status":False, "message":"Device Not Found",'data':json.loads(self.request.body)}               
    if not 'field' in data:
        response = {"status":False, "message":"Field Not Found",'data':json.loads(self.request.body)}               
    if not 'method' in data:
        response = {"status":False, "message":"Method Not Found",'data':json.loads(self.request.body)}               
    if not 'parameters' in data:
        response = {"status":False, "message":"Parameters Not Found",'data':json.loads(self.request.body)}               
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
            sort = ('date_add_server',1)
            query = {}
            field = data["field"]
            method = data["method"]
            params = data["parameters"]
            if "search" in data:
                query = data["search"]
            if 'limit' in data:
                limit = data['limit']
                del data['limit']
                if 'page_number' in data:
                    page_num = data['page_number']
                else:
                    page_num = 1
                skip = limit * (page_num - 1)
            if 'skip' in data:
                skip = data['skip']
            if 'sort' in data:
                sort = (data['sort']['field'],data['sort']['type'])         
            if 'date' in data:
                date_time_str = str(data['date'])
                datesrc_str = datetime.strptime(date_time_str+" 00:00:00",'%Y-%m-%d %H:%M:%S') - td
                datesrc_end = datetime.strptime(date_time_end+" 23:59:59",'%Y-%m-%d %H:%M:%S') - td
                query['date_add_server'] = {"$gte":datesrc_str, "$lt":datesrc_end }
            if 'date_start' in data and 'date_end' in data:
                date_time_str = str(data['date_start'])
                date_time_end = str(data['date_end'])
                if 'time_start'in data and 'time_end' in data:
                    datesrc_str = datetime.strptime(date_time_str+" "+str(data['time_start']),'%Y-%m-%d %H:%M:%S') - td
                    datesrc_end = datetime.strptime(date_time_end+" "+str(data['time_end']),'%Y-%m-%d %H:%M:%S') - td
                else:
                    datesrc_str = datetime.strptime(date_time_str+" 00:00:00",'%Y-%m-%d %H:%M:%S') - td
                    datesrc_end = datetime.strptime(date_time_end+" 23:59:59",'%Y-%m-%d %H:%M:%S') - td
                query['date_add_server'] = {"$gte":datesrc_str, "$lt":datesrc_end }
            
            query["device_code"] = device
            exclude = {'raw_message':0}   
            print(query)         
            result = sensorController.find(collection,query,exclude,limit,skip,sort)
            if not result['status']:
                response = {"status":False, "message":"Data Not Found",'data':json.loads(self.request.body)}               
            else:
                last_time = 0
                last_data = []
                last_filter_data = []
                maxparams = 2
                if method == "bandpass":
                    maxparams = 4
                if method == "kalmanbasic":
                    maxparams = 1
                    P = 0 #Error covariance
                    K = 0 #Kalman Gain
                    H = 1 #measurement map scalar
                    if("H" in params):
                        H = params["H"]
                #-----------------------
                list_sample_time = []
                list_unfiltered = []
                list_filtered = []
                #-----------------------
                for item in result["data"]:
                    ctime = item["date_add_server"]["$date"] / 1000
                    value = item[field]
                    if not filterController.is_float(value) :
                        value = 0 
                        item[str(field)] = 0  
                    else:
                        list_unfiltered.append(value)
                        
                    last_data.append(value)  
                    if(len(last_data)>maxparams):
                        filter_data = value                         
                        if method == "lowpass":
                            filter_data = filterController.scipy_low(params["cutoff"], ctime-last_time,
                                        value, last_data[1], last_data[0],
                                        last_filter_data[1], last_filter_data[0])
                        elif method == "highpass":
                            filter_data = filterController.scipy_high(params["cutoff"], ctime-last_time,
                                        value, last_data[1], last_data[0],
                                        last_filter_data[1], last_filter_data[0])
                        elif method == "bandpass":
                            filter_data = filterController.scipy_band_lfilter(params["low_cutoff"], params["high_cutoff"], ctime-last_time,
                                        value, last_data[3], last_data[2], last_data[1], last_data[0],
                                        last_filter_data[3], last_filter_data[2], last_filter_data[1], last_filter_data[0])
                        elif method == "kalmanbasic":
                            filter_data,K,P = filterController.basic_kalman(value,params["R"],H,K,params["Q"],P,last_data[0])

                        filter_data = float("{:.2f}".format(filter_data))   
                        last_filter_data.append(filter_data)
                        item["filter_"+str(field)] = filter_data #float("{:.2f}".format(filter_data))       
                        list_filtered.append(item["filter_"+str(field)])             
                        del last_data[0]
                        del last_filter_data[0]
                        list_sample_time.append(ctime-last_time)
                    else:
                        last_filter_data.append(value)
                        item["filter_"+str(field)] = value
                   
                    last_time = ctime  
                filter_variance = filterController.variancedata(list_filtered)
                unfilter_variance = filterController.variancedata(list_unfiltered)         
                # print(list_sample_time)
                max_time =  filterController.maxdata(list_sample_time)            
                min_time =  filterController.mindata(list_sample_time)   
                response = {"status":True, 'message':'Success','data':result['data'],'variance':{'filter':filter_variance,'unfilter':unfilter_variance},'sample_time':{'min':min_time,'max':max_time}}

    self.write(response)

class summary(RequestHandler):
  def post(self,device):    
    data = json.loads(self.request.body)
    response = ""
    query = {"device_code":device}
    deviceData = deviceController.findOne(query)
    if not deviceData['status']:
        response = {"status":False, "message":"Device Not Found",'data':json.loads(self.request.body)}               
    if not 'field' in data:
        response = {"status":False, "message":"Field Not Found",'data':json.loads(self.request.body)}               
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
            sort = ('date_add_server',1)
            query = {}
            field = data["field"]
            if "search" in data:
                query = data["search"]
            if 'limit' in data:
                limit = data['limit']
                del data['limit']
                if 'page_number' in data:
                    page_num = data['page_number']
                else:
                    page_num = 1
                skip = limit * (page_num - 1)
            if 'skip' in data:
                skip = data['skip']
            if 'sort' in data:
                sort = (data['sort']['field'],data['sort']['type'])         
            if 'date' in data:
                date_time_str = str(data['date'])
                datesrc_str = datetime.strptime(date_time_str+" 00:00:00",'%Y-%m-%d %H:%M:%S') - td
                datesrc_end = datetime.strptime(date_time_end+" 23:59:59",'%Y-%m-%d %H:%M:%S') - td
                query['date_add_server'] = {"$gte":datesrc_str, "$lt":datesrc_end }
            if 'date_start' in data and 'date_end' in data:
                date_time_str = str(data['date_start'])
                date_time_end = str(data['date_end'])
                if 'time_start'in data and 'time_end' in data:
                    datesrc_str = datetime.strptime(date_time_str+" "+str(data['time_start']),'%Y-%m-%d %H:%M:%S') - td
                    datesrc_end = datetime.strptime(date_time_end+" "+str(data['time_end']),'%Y-%m-%d %H:%M:%S') - td
                else:
                    datesrc_str = datetime.strptime(date_time_str+" 00:00:00",'%Y-%m-%d %H:%M:%S') - td
                    datesrc_end = datetime.strptime(date_time_end+" 23:59:59",'%Y-%m-%d %H:%M:%S') - td
                query['date_add_server'] = {"$gte":datesrc_str, "$lt":datesrc_end }
            
            query["device_code"] = device
            exclude = {'raw_message':0}   
            print(query)         
            result = sensorController.find(collection,query,exclude,limit,skip,sort)
            if not result['status']:
                response = {"status":False, "message":"Data Not Found",'data':json.loads(self.request.body)}               
            else:
                last_time = 0
                #-----------------------
                list_sample_time = []
                list_unfiltered = []
                #-----------------------
                for item in result["data"]:
                    ctime = item["date_add_server"]["$date"] / 1000
                    value = item[field]
                    if not filterController.is_float(value) :
                        value = 0 
                        item[str(field)] = 0  
                    else:
                        list_unfiltered.append(value)                        
                    if( last_time!=0 ):
                        list_sample_time.append(ctime-last_time)
                    last_time = ctime  
                unfilter_variance = filterController.variancedata(list_unfiltered)         
                # print(list_sample_time)
                max_time =  filterController.maxdata(list_sample_time)            
                min_time =  filterController.mindata(list_sample_time)   
                response = {"status":True, 'message':'Success','data':result['data'],'variance':{'unfilter':unfilter_variance},'sample_time':{'min':min_time,'max':max_time}}

    self.write(response)
