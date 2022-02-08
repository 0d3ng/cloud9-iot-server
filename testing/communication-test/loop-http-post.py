import requests
from datetime import datetime
import json
import time
import sys
import random

token = "92h95vj7v81ij80l"
url = "http://localhost:3001/comdata/sensor/"+token+"/" #"http://103.106.72.188:3001/comdata/sensor/"+token+"/"
for x in range(1000):
    # today = datetime.today()
    # msg = {
    #     "device_code":"2k93k2-og05",
    #     "date_add":round(datetime.today().timestamp() * 1000)-700, #today.strftime("%Y-%m-%d %H:%M:%S"),
    #     "gps":{
    #         "latitude":-7.475973 + (random.randint(1,1000) / 10000 ),
    #         "longitude":112.978304 - ( random.randint(1,1000) / 10000 )
    #     },
    #     "temperature": random.randint(2000,3500) / 100,
    #     "fuel":950
    # }
    msg= {
        "temperature_in":14.8,
        "humidity_in":46.4,		
        "di_in":58.5,
        "ac_state":"off",    
        "window_state":1,   
        "door_state":0,   
        "person_num":2   
    }
    if (x > 10 and x < 20) or (x > 50 and x < 58):
        msg= {
            "temperature_in":14.8,
            "humidity_in":46.4,		
            "di_in":58.5,
            "ac_state":"off",    
            "window_state":0,   
            "door_state":1,   
            "person_num":3   
        } 
    payload = json.dumps(msg)
    headers = {
        'Content-Type': 'application/json'
    }
    response = requests.request("POST", url, headers=headers, data = payload)
    print(response.text.encode('utf8'))
    sys.stdout.flush()
    time.sleep(1)
