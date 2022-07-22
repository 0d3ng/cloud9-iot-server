import requests
from datetime import datetime
import json

token = "535k0wfur67i3t8g"#"l8et0xzhu2lb48ah6"
url = "http://localhost:3001/comdata/sensor/"+token+"/" #"http://161.117.58.227:3001/comdata/sensor/"+token+"/" #
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
payload = "::rc=80000000:lq=63:ct=76C1:ed=810D731C:id=3:ba=2840:a1=1304:a2=0672:x=-002:y=0005:z=0095"
headers = {
    'Content-Type': 'text/plain'
}
response = requests.request("POST", url, headers=headers, data = payload)
print(response.elapsed.total_seconds())
print(response.text.encode('utf8'))
    
