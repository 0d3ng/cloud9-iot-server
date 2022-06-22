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
    ['simulation/([^/]+)/','simulation'],
    ['summary/([^/]+)/','summary']
]

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
                            filter_data = filterController.scipy_band(params["low_cutoff"], params["high_cutoff"], ctime-last_time,
                                        value, last_data[3], last_data[2], last_data[1], last_data[0],
                                        last_filter_data[3], last_filter_data[2], last_filter_data[1], last_filter_data[0])
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


class simulationOld(RequestHandler):
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
                maxparams = 2
                last_data = []
                last_filter_data = []
                if method == "bandpass":
                    maxparams = 4
                for item in result["data"]:
                    ctime = item["date_add_server"]["$date"] / 1000
                    value = item[field]
                    if not filterController.is_float(value) :
                        value = 0 
                        item[str(field)] = 0  
                    
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
                            filter_data = filterController.scipy_high(params["low_cutoff"], params["high_cutoff"], ctime-last_time,
                                        value, last_data[3], last_data[2], last_data[1], last_data[0],
                                        last_filter_data[3], last_filter_data[2], last_filter_data[1], last_filter_data[0])
                        last_filter_data.append(filter_data)
                        item["filter_"+str(field)] = float("{:.2f}".format(filter_data))                    
                        del last_data[0]
                        del last_filter_data[0]
                    else:
                        last_filter_data.append(value)
                        item["filter_"+str(field)] = value
                    
                    last_time = ctime  
                response = {"status":True, 'message':'Success','data':result['data']}

    self.write(response)
