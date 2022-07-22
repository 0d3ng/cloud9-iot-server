import requests
from datetime import datetime
import json

# today = datetime.today()
# msg = {
#     "device_code":"xzhu2l-rc01",
#     "date_add":round(datetime.today().timestamp() * 1000), #today.strftime("%Y-%m-%d %H:%M:%S"),
#     "gps":{
#         "latitude":-7.475973,
#         "longitude":112.978304
#     },
#     "temperature": 25.5,
#     "fuel":1000
# }
# payload = json.dumps(msg)
# headers = {
#     'Content-Type': 'application/json'
# }
# response = requests.request("POST", url, headers=headers, data = payload)

# headers = {
#     'Content-Type': 'text/plain'
# }
# response = requests.request("POST", url, headers=headers, data = payload)
# cUnix2 = int(datetime.now().timestamp() * 1000)
# print(response.elapsed.total_seconds())
# print(cUnix2-cUnix)
# print(response.text.encode('utf8'))

token = "m809z774n7qk89ux"#"l8et0xzhu2lb48ah6"
url = "http://103.106.72.188:3001/comdata/sensor/"+token+"/" #"http://localhost:3001/comdata/sensor/"+token+"/" #
def sendData(payload):
    cUnix = int(datetime.now().timestamp() * 1000)
    cDate = datetime.now().strftime("%Y-%m-%d")
    cTime = datetime.now().strftime("%H:%M:%S")
    headers = {
        'Content-Type': 'text/plain'
    }
    response = requests.request("POST", url, headers=headers, data = payload)
    cUnix2 = int(datetime.now().timestamp() * 1000)
    writeLog("HTTP_"+str(cDate),str(cUnix2-cUnix)+","+cDate+","+cTime+","+str(cUnix)+","+str(cUnix2)+","+payload)

def writeLog(file,value):
    with open("log"+file+".log",'a+',newline='') as f:
        f.write(value+'\n')
    
payload = "::rc=80000000:lq=63:ct=76C1:ed=810D731C:id=3:ba=2840:a1=1304:a2=0672:x=-002:y=0005:z=0095"
sendData(payload)