import requests
from datetime import datetime
import json
import time
import sys
import random


token = "89pfg6ixg29f1y25" 
url = "http://103.106.72.188:3001/comdata/sensor/"+token+"/" 
def delimeterExtract(msg):
    filter = msg.split(":")
    result = {}
    for val in filter:
        item = val.split("=")
        if(len(item) == 2):  
            key = item[0].strip()
            try:
                value = float(item[1])
            except:
                value = item[1]
            result[key] = value
    return result

def sendHTTP(stringData):
    msg = delimeterExtract(stringData)
    payload = json.dumps(msg)
    headers = {
        'Content-Type': 'application/json'
    }
    now = datetime.now()
    cDate = now.strftime("%Y-%m-%d")
    cTime = now.strftime("%H:%M:%S")
    print(payload)
    # cUnix = int(now.timestamp() * 1000)
    # response = requests.request("POST", url, headers=headers, data = payload)
    # cUnixEnd = int(now.timestamp() * 1000)
    # diff = cUnixEnd-cUnix
    # print(msg,cUnix,cUnixEnd,diff) 
    # start = time.perf_counter()

    response = requests.post(url, headers=headers, data=payload)
    print(response.elapsed.total_seconds())
    # request_time = time.perf_counter() - start
    # print("Request completed in {0:.0f}ms".format(request_time))



for x in range(1000):
    st = "::rc=80000000:lq=81:ct=909E:ed=810D731C:id=3:ba=2880:a1=1312:a2=0680:x=-011:y=0005:z=0095"
    sendHTTP(st)
    

