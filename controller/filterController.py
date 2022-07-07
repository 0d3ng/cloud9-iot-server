import sys
from bson import ObjectId
import json 
from function import *
import datetime
import pandas as pd
from controller import schemaDataController
from controller import sensorController
from pytz import timezone
import statistics
import numpy as np
import matplotlib.pyplot as plt
from scipy.signal import buttap, lp2hp_zpk, bilinear_zpk, zpk2tf, butter, lfilter

from configparser import ConfigParser

from function import cloud9Lib
config = ConfigParser()
config.read("config.ini")

sensors = []
db = db.dbmongo()
td = datetime.timedelta(hours=int(config["SERVER"]["timediff"]))

collection = "filter"
prefix_collection_sensor = "sensor_data_"
prefix_collection_schema = "schema_data_"

def variancedata(datalist, defaultval=0):
    try:
        df = pd.DataFrame(datalist)
        datalist = df[df[0].apply(lambda x: is_float(x))]
        return statistics.variance(datalist[0])
    except:
        return defaultval

def maxdata(datalist, defaultval=0):
    try:
        df = pd.DataFrame(datalist)
        datalist = df[df[0].apply(lambda x: is_float(x))]
        return max(datalist[0])
    except:
        return defaultval

def mindata(datalist, defaultval=0):
    try:
        df = pd.DataFrame(datalist)
        datalist = df[df[0].apply(lambda x: is_float(x))]
        return min(datalist[0])
    except:
        return defaultval

def is_float(x):
    try:
        float(x)
    except:
        return False
    return True

def scipy_low(cutoff_freq, sample_time, x0, x1, x2, y1, y2):
    sample_rate = 1.0 / sample_time
    nyquist_freq = 0.5 * sample_rate
    # nyquist normalized cutoff for digital design
    #1/4 -> 0.25
    #0.5 * 0.25 -> 0.125
    #cutoff can't offer thatn 0.125
    Wn = cutoff_freq / nyquist_freq
    b, a = butter(2, Wn, btype='lowpass')
    # print("a[1] ",type(a[1]))
    # print("a[2] ",type(a[2]))
    # print("b[0] ",type(b[0]))
    # print("b[1] ",type(b[1]))
    # print("b[2] ",type(b[2]))
    # print("y1 ",type(y1))
    # print("y2 ",type(y2))
    # print("x0 ",type(x0))
    # print("x1 ",type(x1))
    # print("x2 ",type(x2))

    return -a[1] * y1 - a[2] * y2 + b[0] * x0 + b[1] * x1 + b[2] * x2

def scipy_high(cutoff_freq, sample_time, x0, x1, x2, y1, y2):
    sample_rate = 1.0 / sample_time
    nyquist_freq = 0.5 * sample_rate
    # nyquist normalized cutoff for digital design
    #1/4 -> 0.25
    #0.5 * 0.25 -> 0.125
    #cutoff can't offer thatn 0.125
    Wn = cutoff_freq / nyquist_freq
    b, a = butter(2, Wn, btype='highpass')
    return -a[1] * y1 - a[2] * y2 + b[0] * x0 + b[1] * x1 + b[2] * x2

def scipy_band(cutoff_freq_low,cutoff_freq_high, sample_time, x0, x1, x2, x3, x4, y1, y2, y3, y4):
    sample_rate = 1.0 / sample_time
    nyquist_freq = 0.5 * sample_rate
    high = cutoff_freq_high / nyquist_freq
    low = cutoff_freq_low / nyquist_freq
    b, a = butter(2, [low,high], btype='bandstop',analog=False)
    return  -a[1] * y1 - a[2] * y2 -a[3] * y3 - a[4] * y4 +  b[0] * x0 + b[1] * x1 + b[2] * x2 + b[3] * x3 + b[4] * x4

def scipy_band_lfilter(cutoff_freq_low,cutoff_freq_high, sample_time, x0, x1, x2, x3, x4, y1, y2, y3, y4):
    sample_rate = 1.0 / sample_time
    nyquist_freq = 0.5 * sample_rate
    high = cutoff_freq_high / nyquist_freq
    low = cutoff_freq_low / nyquist_freq
    b, a = butter(2, [low,high], btype='bandpass', analog=False)
    data = [x4, x3, x2, x1, x0]
    res = lfilter(b, a, data)
    result = res[4]
    return result

def basic_kalman(U,R,H,K,Q,P,U_hat):
    K = P*H / ( H*P*H + R )
    U_hat = U_hat + K*( U-H*U_hat)
    P = (1-K*H)*P + Q
    return U_hat,K,P

def add(fillData):  
    insertQuery = {
        'filter_code':fillData.get('filter_code', None),        
        'device':fillData.get('device', None),
        'field':fillData.get('field', None), #field name only
        'date_add': datetime.datetime.utcnow(),
        'add_by':fillData.get('add_by', None),
        'stream':fillData.get('stream', False),
        'waiting_time':fillData.get('waiting_time', None), #in second --> don't show it
        'method':fillData.get('method', None), #array[{method name, parameter}]
        'group_data':fillData.get('group_data', None), #array[field_name] 
        'rules_data':fillData.get('rules_data', None) #array[{field_name,groupby}] -> to filtering spesific value... [not really implemented]       
    }
    result = db.insertData(collection,insertQuery)
    if result == []:
        response = {'status':False, 'message':"Add Failed"}               
    else:
        response = {'status':True,'message':'Success','data':result}
        if insertQuery['stream'] == True:
            triggerService(insertQuery["filter_code"],True)
    return cloud9Lib.jsonObject(response)

def find(query):  
    result = db.find(collection,query)
    if result == []:
        response = {"status":False, "data":query}               
    else:
        response = {'status':True, 'data':result}    
    return cloud9Lib.jsonObject(response)

def findOne(query):  
    result = db.findOne(collection,query, None)
    if result is None or result is False:
        response = {"status":False, "data":query}               
    else:
        response = {'status':True,'message':'Success','data':result}    
    return cloud9Lib.jsonObject(response)

def update(query,data):            
    updateData = {}
    queryUpdate = {}
    if 'filter_code' in query: queryUpdate['filter_code'] = query['filter_code']
    if '_id' in query: queryUpdate['_id'] = query['_id']

    if 'filter_code' in data: updateData['filter_code'] = data['filter_code']
    if 'device' in data: updateData['device'] = data['device']
    if 'field' in data: updateData['field'] = data['field']
    if 'stream' in data: updateData['stream'] = data['stream']
    if 'waiting_time' in data: updateData['waiting_time'] = data['waiting_time']
    if 'method' in data: updateData['method'] = data['method']
    if 'group_data' in data: updateData['group_data'] = data['group_data']
    if 'rules_data' in data: updateData['rules_data'] = data['rules_data']
    if 'updated_by' in data: updateData['updated_by'] = data['updated_by']

    if updateData == []:
        return {"status":False, "message":"UPDATE NONE"}   
    last = findOne(queryUpdate)['data'] 
        
    result = db.updateData(collection,queryUpdate,updateData)
    if not result :
        response = {"status":False, "message":"UPDATE FAILED"}               
    else:
        response = {'status':True,'message':'Success','data':result}
        if last['stream'] !=  updateData['stream']:
            if updateData['stream'] == True:
                triggerService(last["filter_code"],True)
            if updateData['stream'] == False:
                triggerService(last["filter_code"],False)
    return cloud9Lib.jsonObject(response)

def delete(query): 
    listData = findOne(query)['data']           
    result = db.deleteData(collection,query)
    if not result:
        response = {"status":False, "message":"DELETE FAILED"}               
    else:
        response = {'status':True,'message':'Success','data':result}
        triggerService(listData["filter_code"],False)
    return cloud9Lib.jsonObject(response)

def triggerService(filter_code,status=True):
    send = {
        "filter_code":filter_code
    }
    print("----------------------")
    print(send)
    if status :
        print(config["MQTT"]["filter_start"])
        mqttcom.publish(config["MQTT"]["filter_start"],send)
        return
    else:
        print(config["MQTT"]["filter_remove"])
        mqttcom.publish(config["MQTT"]["filter_remove"],send)
        return