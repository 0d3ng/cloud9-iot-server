import json,requests 
token = "92h95vj7v81ij80l"
url = "http://localhost:3001/comdata/sensor/"+token+"/"
msg= {
    "temperature_in":14.8,
    "humidity_in":46.4,		
    "di_in":58.5,
    "ac_state":"off",    
    "window_state":1,   
    "door_state":0,   
    "person_num":2   
}
payload = json.dumps(msg)
headers = {
    'Content-Type': 'application/json'
}
requests.request("POST", url, headers=headers, data = payload)