import sys

from pymongo import collection
sys.path.append('../../')
from function import cloud9Lib
import json 
from function import *
from controller import sensorController
from datetime import datetime
from datetime import timedelta
import pandas as pd

td = timedelta(hours=9)
prefix_collection = "sensor_data_"

f = open('field.json')
field = json.load(f)["field"]

fieldKey = ""

def getdata(time_str,code,key,value):
    collection = prefix_collection+str(code)
    datesrc_str = datetime.strptime(time_str+":00",'%Y-%m-%d %H:%M:%S') - td
    datesrc_end = datetime.strptime(time_str+":59",'%Y-%m-%d %H:%M:%S') - td
    query = {
        'date_add_server' : {"$gte":datesrc_str, "$lt":datesrc_end }
    }
    exclude = {
        str(key):1,
        str(value):1,
        "date_add_server":14
    }
    result = sensorController.find(collection,query,exclude)
    if not result['status']:
        response = []               
    else:
        response = []
        for item in result['data']:
            response.append([item[str(key)],item[str(value)]])
    return response

def grouping(datalist):
    d = {}
    for key, val in datalist:
        if not key  in d:
            d[key] = []
        d[key].append(val)

    return d 

def averagedata(datalist):
    for key in datalist:
        datalist[key] = sum(datalist[key]) / len(datalist[key])
    return datalist 

timesrc = "2021-12-15 17:47"
insertData = {}
for fieldData in field:
    fieldName = list(fieldData.keys())[0]
    fieldValue = fieldData[fieldName]
    if(str(fieldValue) != "key"):
        code = fieldValue["data"][0]
        field_val_search = fieldValue["data"][1]
        field_key_search = fieldValue["data"][2]
        method = fieldValue["option"]
        datalist = getdata(timesrc,code,field_key_search,field_val_search)
        datalist = grouping(datalist)
        if(method == "average"):
            datalist = averagedata(datalist)
        for key in datalist:
            if not key in insertData:
                insertData[key] = {}
            insertData[key].update({fieldName:datalist[key]})
for itemsKey in insertData:
    insertQuery = {}
    itemData = insertData[itemsKey]
    for fieldData in field:
        fieldName = list(fieldData.keys())[0]
        fieldValue = fieldData[fieldName]
        if(str(fieldValue) == "key"):
            insertQuery[fieldName] = itemsKey
        else:
            if(not fieldName in itemData):
                insertQuery[fieldName] = fieldValue["default"]
            else:
                insertQuery[fieldName] = itemData[fieldName]
    print(insertQuery)
print("--------------------------------")
sys.stdout.flush()

####Select data by pada tanggal, selectnya id sama field yang dituju, disimpan pada array dengan key --> field=key, valnya --> field_val. simpan di numpy
####buat rata2 per key, lalu simpan ke data schema bebas sesuai key yg ada.