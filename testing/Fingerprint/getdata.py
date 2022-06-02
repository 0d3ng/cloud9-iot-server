import sys
from bson import ObjectId
import json 
import datetime
import pandas as pd
from pytz import timezone
import statistics
import math
import requests
import time
import csv

restapi = "http://103.106.72.188:3001/"

def HTTP_post(url,message):
    payload = json.dumps(message)
    headers = {
        'Content-Type': 'application/json'
    }
    response = requests.request("POST", url, headers=headers, data = payload)
    return json.loads(response.text.encode('utf8'))


def count(code,query):
    url = restapi+"device/data/"+code+"/count/"
    return HTTP_post(url,query)

def get(code,query):
    url = restapi+"device/data/"+code+"/"
    return HTTP_post(url,query)

def writeCSV(file,value):
    # value = list(value.values())
    with open(file+".csv",'a+', newline='') as f:
        writer=csv.writer(f)
        writer.writerow(value)

date_start = "2022-05-19"
date_end = "2022-05-20"
time_start = "21:45:00"
time_end = "1:45:00"
limit = 1000
sort = {
    "field":"date_add_server",
    "type" :1 #1-> asc, 0->desc
}   
skip=0
query = {
    "date_start":date_start,
    "date_end":date_end,
    "time_start":time_start,
    "time_end":time_end,
    "sort":sort
}

#list
list = {
    "receiver1":"vy00",
    "receiver2":"zi05",
    "receiver3":"wc64",
    "receiver4":"as50",
    "receiver5":"dn56",
    "receiver6":"wz48"
}

#field
field_format = ["id","lq","x","y","z"]


filen = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')

for name in list:
    code = list[name]
    print(name," ",code)
    total = count(code,query)["data"]
    page = math.ceil(total/limit)
    print(page)
    for x in range(page) :
        query["limit"] = limit
        query["skip"] = x * limit
        data = get(code,query)
        for item in data["data"]:
            itemData = []
            ts = datetime.datetime.fromtimestamp(int(item['date_add_server']['$date'])/1000)
            dates = ts.strftime("%Y-%m-%d")
            times = ts.strftime("%H:%M:%S")
            itemData.append(dates)
            itemData.append(times)
            for key in field_format: 
                if key in item:
                    itemData.append(item[key])
            writeCSV(code+" - "+name,itemData)
    
