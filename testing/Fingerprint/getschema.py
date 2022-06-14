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
    url = restapi+"schema/data/"+code+"/count/"
    return HTTP_post(url,query)

def get(code,query):
    url = restapi+"schema/data/"+code+"/"
    return HTTP_post(url,query)

def writeCSV(file,value):
    # value = list(value.values())
    with open(file+".csv",'a+', newline='') as f:
        writer=csv.writer(f)
        writer.writerow(value)

# date_start = "2022-06-02"
date_start = "2022-06-02"
date_end = "2022-06-04"
# time_start = "16:31:08"
time_start = "16:31:08"
time_end = "21:55:38"
limit = 1000
sort = {
    "field":"date_add_auto",
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
    # "5s data":"0kw38j"#,
    "10s data":"m8iy5a"
}

#field
field_format = ["id","lq1","lq2","lq3","lq4","lq5","lq6"]

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
            ts = datetime.datetime.fromtimestamp(int(item['date_add_auto']['$date'])/1000)
            dates = ts.strftime("%Y-%m-%d")
            times = ts.strftime("%H:%M:%S")
            itemData.append(dates)
            itemData.append(times)
            for key in field_format: 
                if key in item:
                    itemData.append(item[key])
            writeCSV(code+" - "+name,itemData)
        print("Progress: "+str(x+1)+"/"+str(page))
    
