from email.policy import default
import sys

from pymongo import collection
sys.path.append('../../')
from function import cloud9Lib
import json 
from function import *
from controller import schemaDataController
import datetime
from datetime import timedelta
import pandas as pd
from pytz import timezone
import statistics

db = db.dbmongo()
td = timedelta(hours=9)
prefix_collection_sensor = "sensor_data_"
prefix_collection_schema = "schema_data_"

f = open('field2.json')
field = json.load(f)["field"]
schema_code = "u834oe"
batch_code = None
send_result = None

# timesrc1 = "2022-02-04 09:47:00"
# timesrc2 = "2022-02-04 09:48:00"
# timesrc1 = datetime.strptime(timesrc1,'%Y-%m-%d %H:%M:%S')
# timesrc2 = datetime.strptime(timesrc2,'%Y-%m-%d %H:%M:%S')
# pipeline = [{"$unwind":"$id" },
#             {"$match":{
#                 'date_add_server': {"$gte": timesrc1,"$lt": timesrc2},
#                 "$and":[{"id":{"$ne":None}},{"id":{"$ne":""}}]
#             }},
#             {"$sort" : { 'date_add_server' : -1 }},
#             {"$group":{
#                 "_id":"$id",
#                 "lq": {"$push":"$lq"},
#                 "accel": {"$push":"$accel"}
#             }}]
# ##Result urutannya terbalik.
# res = db.aggregate("sensor_data_aa83",pipeline)
# print(res)
# sys.stdout.flush()


def averagedata(datalist,defaultval):
    try:
        df = pd.DataFrame(datalist)
        datalist = df[df[0].apply(lambda x: is_float(x))]
        return statistics.mean(datalist[0])
    except:
        print(datalist)
        print("ERROR Average")
        return defaultval

def variancedata(datalist,defaultval):
    try:
        df = pd.DataFrame(datalist)
        datalist = df[df[0].apply(lambda x: is_float(x))]
        return statistics.variance(datalist[0])
    except:
        return defaultval

def firstdata(datalist,defaultval):
    try:
        return datalist[0]
    except:
        return defaultval

def lastdata(datalist,defaultval):
    try:
        return datalist[len(datalist)]
    except:
        return defaultval

def is_float(x):
    try:
        float(x)
    except ValueError:
        return False
    return True

def getSensorData(time_str,time_end,code,key,value,collectid=None):
    if(collectid):
        collection = prefix_collection_sensor+str(collectid)
    else:
        collection = prefix_collection_sensor+str(code)
    datesrc_str = datetime.datetime.strptime(time_str,'%Y-%m-%d %H:%M:%S') - td
    datesrc_end = datetime.datetime.strptime(time_end,'%Y-%m-%d %H:%M:%S') - td
    query = {
        'date_add_server': {"$gte": datesrc_str,"$lt": datesrc_end},
        "$and":[{str(key):{"$ne":None}},{str(key):{"$ne":""}}]
    }    
    if(collectid):
        query["device_code"] = str(code)        
    
    group = {
        "_id":"$"+str(key)                
    }
    for item in value:
        group[str(item)] = {"$push":"$"+str(item)}

    pipeline = [{"$unwind":"$"+str(key) },
            {"$match":query},
            {"$sort" : { 'date_add_server' : -1 }},
            {"$group":group}]
    # print("--------------------")
    # print(collection)
    # print(pipeline)    
    result = db.aggregate(collection,pipeline)
    response = {}
    itemkey = []
    for item in result:
        response[item[str(key)]] = item
        itemkey.append(item[str(key)])
    # print(response)
    # print("-------++++---------")
    sys.stdout.flush()
    return response,itemkey

timesrc1 = "2022-02-04 18:47:00"
timesrc2 = "2022-02-04 18:58:00"
# res = getSensorData(timesrc1,timesrc2,"aa83","id",["lq","accel"])

# insertCount = 0
# dataSource = {}
# listKey = []
# a = datetime.datetime.now()
# print("start")
# for fieldData in field:
#     fieldName = list(fieldData.keys())[0]
#     fieldValue = fieldData[fieldName]
#     if(str(fieldValue) != "key"):
#         code = fieldValue["data"][0]
#         field_val_search = fieldValue["data"][1]
#         field_key_search = fieldValue["data"][2]
#         if(code not in dataSource):
#             dataSource[code] = {}
#         if(field_key_search not in dataSource[code]):
#             dataSource[code][field_key_search] = []
#         if(field_val_search not in dataSource[code][field_key_search]):
#             dataSource[code][field_key_search].append(field_val_search)            
# for code in dataSource:
#     for key_search in dataSource[code]:
#         dataSource[code][key_search],itemKey = getSensorData(timesrc1,timesrc2,code,key_search,dataSource[code][key_search])
#         listKey= listKey + list(set(itemKey)-set(listKey))

# dataSource = pd.DataFrame(dataSource)
# dataSource = dataSource.dropna()
# for itemKey in listKey:
#     insertQuery = {}
#     for fieldData in field:
#         fieldName = list(fieldData.keys())[0]
#         fieldValue = fieldData[fieldName]
#         if(str(fieldValue) == "key"):
#             insertQuery[fieldName] = itemKey
#         else:
#             code = fieldValue["data"][0]
#             field_val_search = fieldValue["data"][1]
#             field_key_search = fieldValue["data"][2]
#             method = fieldValue["option"]
#             if itemKey in dataSource[code][field_key_search]:
#                 item_ds = dataSource[code][field_key_search][itemKey][field_val_search]
#                 if method == 'average':
#                     insertQuery[fieldName] = averagedata(item_ds,fieldValue["default"])
#                 elif method == 'variance':
#                     insertQuery[fieldName] = variancedata(item_ds,fieldValue["default"])
#                 elif method == 'lastvalue':
#                     insertQuery[fieldName] = lastdata(item_ds,fieldValue["default"])
#                 elif method == 'fisrtvalue':
#                     insertQuery[fieldName] = firstdata(item_ds,fieldValue["default"])
#                 else:
#                     insertQuery[fieldName] = fieldValue["default"]
#             else:
#                 insertQuery[fieldName] = fieldValue["default"]

#     filter = schemaDataController.filter(schema_code,insertQuery)
#     if filter['status']:
#         insertQuery = filter['data']
#         if batch_code is not None :
#             insertQuery["batch_code"] = batch_code
#         if send_result is None:
#             insertQuery["date_add_batch"] = datetime.datetime.now(timezone('Asia/Tokyo'))
#         insertQuery["date_add_auto"] = datetime.datetime.strptime(timesrc2,'%Y-%m-%d %H:%M:%S') - td            
#         try:
#             insert = schemaDataController.add(prefix_collection_schema+schema_code,insertQuery)
#             if insert['status']: 
#                 insertCount = insertCount + 1
#         except:
#             print(schema_code)
#             print(insertQuery)
#             print("ERROR")
#             sys.stdout.flush()        

# b = datetime.datetime.now()        
# c = b - a
# print(c.total_seconds())
# print(c.microseconds)
# import json
# jsonString = json.dumps(dataSource)
# jsonFile = open("data.json", "w")
# jsonFile.write(jsonString)
# jsonFile.close() 

# df = pd.DataFrame([36, 42, 36, 36, None,36, 33, 36])
# print(df)
# df = df[df[0].apply(lambda x: is_float(x))]
# df = df.dropna()
# print(df)

def generateDate(time_str,time_end,freq):
    time_str = datetime.datetime.strptime(time_str,'%Y-%m-%d %H:%M:%S')
    time_end = datetime.datetime.strptime(time_end,'%Y-%m-%d %H:%M:%S')
    time_str = time_str.strftime('%Y-%m-%dT%H:%M:%SZ')
    time_end = time_end.strftime('%Y-%m-%dT%H:%M:%SZ')
    freq = str(freq)+"S"
    l = (pd.DataFrame(columns=['NULL'],
                    index=pd.date_range(time_str, time_end,freq=freq))
        .index.strftime('%Y-%m-%d %H:%M:%S')
        .tolist()
    )
    return l

print(generateDate(timesrc1,timesrc2,30))