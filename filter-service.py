from cmath import e
import multiprocessing
import threading
import sys, json, time, os
import paho.mqtt.client as mqttClient #Must Install Req
from function import *
from controller import filterController,deviceController
from datetime import datetime,timedelta
from configparser import ConfigParser
from importlib import reload
from bson import ObjectId
from pytz import timezone

config = ConfigParser()
config.read("config.ini")
#Config
Connected = False
broker_address= config["MQTT"]["broker"]
port = int(config["MQTT"]["port"])                         
user = config["MQTT"]["user"]
password = config["MQTT"]["pass"]
config_folder = "filter_config"
topic_list = {}

def on_connect(client, userdata, flags, rc):
    print("rc: "+str(rc))
    sys.stdout.flush()
    if rc == 0:
        print("Connected to broker")
        print("------------------------------------")
        client.subscribe(config["MQTT"]["filter_start"])
        client.subscribe(config["MQTT"]["filter_remove"])
        print(config["MQTT"]["filter_start"]," - ",config["MQTT"]["filter_remove"])
        filter_list()           
        # topic = "mqtt/output/device-vy00"

        # inputs = '{"topic": "/Project_IPS/client1", "channel_type": "mqtt", "date_add_server": 1657015523429, "device_code": "vy00", "id": "3", "lq": 39, "x": -15.0, "y": 9.0, "z": 91.0, "accel": 97.54486147409304, "receive_unix_time": 1657015523410, "save_unix_time": 1657015523437, "_id": "62c40ce31cd75442ec3b8e1a"}'
        # inputs = json.loads(inputs)    
        # on_process(topic,inputs)       
        # inputs = '{"topic": "/Project_IPS/client1", "channel_type": "mqtt", "date_add_server": 1657015523429, "device_code": "vy00", "id": "2", "lq": 39, "x": -15.0, "y": 9.0, "z": 92.0, "accel": 97.54486147409304, "receive_unix_time": 1657015523410, "save_unix_time": 1657015523437, "_id": "62c40ce31cd75442ec3b8e1a"}'
        # inputs = json.loads(inputs)   
        # on_process(topic,inputs)        
        # inputs = '{"topic": "/Project_IPS/client1", "channel_type": "mqtt", "date_add_server": 1657015523429, "device_code": "vy00", "id": "1", "lq": 39, "x": -15.0, "y": 9.0, "z": 93.0, "accel": 97.54486147409304, "receive_unix_time": 1657015523410, "save_unix_time": 1657015523437, "_id": "62c40ce31cd75442ec3b8e1a"}'
        # inputs = json.loads(inputs)
        # on_process(topic,inputs)
        
        # inputs = '{"topic": "/Project_IPS/client1", "channel_type": "mqtt", "date_add_server": 1657015525429, "device_code": "vy00", "id": "3", "lq": 39, "x": -25.0, "y": 91.0, "z": 77.0, "accel": 97.54486147409304, "receive_unix_time": 1657015523410, "save_unix_time": 1657015523437, "_id": "62c40ce31cd75442ec3b8e1a"}'
        # inputs = json.loads(inputs)
        # on_process(topic,inputs)
        # inputs = '{"topic": "/Project_IPS/client1", "channel_type": "mqtt", "date_add_server": 1657015525429, "device_code": "vy00", "id": "2", "lq": 39, "x": -85.0, "y": 91.0, "z": 57.0, "accel": 97.54486147409304, "receive_unix_time": 1657015523410, "save_unix_time": 1657015523437, "_id": "62c40ce31cd75442ec3b8e1a"}'
        # inputs = json.loads(inputs)
        # on_process(topic,inputs)
        # inputs = '{"topic": "/Project_IPS/client1", "channel_type": "mqtt", "date_add_server": 1657015525429, "device_code": "vy00", "id": "1", "lq": 39, "x": -55.0, "y": 91.0, "z": 67.0, "accel": 97.54486147409304, "receive_unix_time": 1657015523410, "save_unix_time": 1657015523437, "_id": "62c40ce31cd75442ec3b8e1a"}'
        # inputs = json.loads(inputs)
        # on_process(topic,inputs)

        # inputs = '{"topic": "/Project_IPS/client1", "channel_type": "mqtt", "date_add_server": 1657015527429, "device_code": "vy00", "id": "3", "lq": 39, "x": 5.0, "y": 3.0, "z": 92.0, "accel": 97.54486147409304, "receive_unix_time": 1657015523410, "save_unix_time": 1657015523437, "_id": "62c40ce31cd75442ec3b8e1a"}'
        # inputs = json.loads(inputs)
        # on_process(topic,inputs)
        # inputs = '{"topic": "/Project_IPS/client1", "channel_type": "mqtt", "date_add_server": 1657015527429, "device_code": "vy00", "id": "2", "lq": 39, "x": 5.0, "y": 3.0, "z": 91.0, "accel": 97.54486147409304, "receive_unix_time": 1657015523410, "save_unix_time": 1657015523437, "_id": "62c40ce31cd75442ec3b8e1a"}'
        # inputs = json.loads(inputs)
        # on_process(topic,inputs)
        # inputs = '{"topic": "/Project_IPS/client1", "channel_type": "mqtt", "date_add_server": 1657015527429, "device_code": "vy00", "id": "1", "lq": 39, "x": 5.0, "y": 3.0, "z": 92.0, "accel": 97.54486147409304, "receive_unix_time": 1657015523410, "save_unix_time": 1657015523437, "_id": "62c40ce31cd75442ec3b8e1a"}'
        # inputs = json.loads(inputs)
        # on_process(topic,inputs)
        
        # inputs = '{"topic": "/Project_IPS/client1", "channel_type": "mqtt", "date_add_server": 1657015529429, "device_code": "vy00", "id": "3", "lq": 39, "x": 31.0, "y": -9.0, "z": 12.0, "accel": 97.54486147409304, "receive_unix_time": 1657015523410, "save_unix_time": 1657015523437, "_id": "62c40ce31cd75442ec3b8e1a"}'
        # inputs = json.loads(inputs)
        # on_process(topic,inputs)
        # inputs = '{"topic": "/Project_IPS/client1", "channel_type": "mqtt", "date_add_server": 1657015529429, "device_code": "vy00", "id": "2", "lq": 39, "x": 25.0, "y": -9.0, "z": 14.0, "accel": 97.54486147409304, "receive_unix_time": 1657015523410, "save_unix_time": 1657015523437, "_id": "62c40ce31cd75442ec3b8e1a"}'
        # inputs = json.loads(inputs)
        # on_process(topic,inputs)
        # inputs = '{"topic": "/Project_IPS/client1", "channel_type": "mqtt", "date_add_server": 1657015529429, "device_code": "vy00", "id": "1", "lq": 39, "x": 16.0, "y": -9.0, "z": 15.0, "accel": 97.54486147409304, "receive_unix_time": 1657015523410, "save_unix_time": 1657015523437, "_id": "62c40ce31cd75442ec3b8e1a"}'
        # inputs = json.loads(inputs)
        # on_process(topic,inputs)

        
        # inputs = '{"topic": "/Project_IPS/client1", "channel_type": "mqtt", "date_add_server": 1657015531429, "device_code": "vy00", "id": "3", "lq": 39, "x": -5.0, "y": 15.0, "z": 44.0, "accel": 97.54486147409304, "receive_unix_time": 1657015523410, "save_unix_time": 1657015523437, "_id": "62c40ce31cd75442ec3b8e1a"}'
        # inputs = json.loads(inputs)
        # on_process(topic,inputs)
        # inputs = '{"topic": "/Project_IPS/client1", "channel_type": "mqtt", "date_add_server": 1657015531429, "device_code": "vy00", "id": "2", "lq": 39, "x": -5.0, "y": 11.0, "z": 43.0, "accel": 97.54486147409304, "receive_unix_time": 1657015523410, "save_unix_time": 1657015523437, "_id": "62c40ce31cd75442ec3b8e1a"}'
        # inputs = json.loads(inputs)
        # on_process(topic,inputs)
        # inputs = '{"topic": "/Project_IPS/client1", "channel_type": "mqtt", "date_add_server": 1657015531429, "device_code": "vy00", "id": "1", "lq": 39, "x": -5.0, "y": 12.0, "z": 23.0, "accel": 97.54486147409304, "receive_unix_time": 1657015523410, "save_unix_time": 1657015523437, "_id": "62c40ce31cd75442ec3b8e1a"}'
        # inputs = json.loads(inputs)
        # on_process(topic,inputs)

    else:
        print("Connection failed")
        sys.stdout.flush()


def on_message(client, userdata, message):
    raw_msg = message.payload.decode("utf-8")
    try:
        raw_object = json.loads(raw_msg)
        if message.topic == config["MQTT"]["filter_start"] :
            on_service(raw_object)
        elif message.topic == config["MQTT"]["filter_remove"] :
            on_remove(raw_object)
        else:
            on_process(message.topic,raw_object)
    except Exception as e:
        print("error:")
        print(e)
        raw_object = {"failed":True}

def on_service(message):
    if("filter_code" in message):
        code = message["filter_code"]
        query = {"filter_code":code}
        result = filterController.findOne(query)
        print(result)    
        if result['status']:
            run_filter_service(result['data'])

    
def on_remove(message):
    if("filter_code" in message):
        code = message["filter_code"]
        query = {"filter_code":code}
        result = filterController.findOne(query)  
        print(result)  
        if result['status']:
            item = result['data'] 
            topic_subs = "mqtt/output/device-"+item["device"]
            remove = remove_config(code)
            print(remove)
            if(remove):
                print("Remove Filter code "+code+": Device "+item["device"]+" - "+item["field"]+" - "+item["method"]["name"])
                if(code in topic_list[topic_subs]):
                    del topic_list[topic_subs][topic_list[topic_subs].index(code)]
                if( len(topic_list[topic_subs]) == 0 ):
                    client.unsubscribe(topic_subs)
                    print("Unsubscribe : "+topic_subs)
                

def on_process(topic,message):
    for code in topic_list[topic]:
        filecon = get_config(code)
        config = filecon["config"]
        data = filecon["data"]
        method = config["method"]
        groupby = config["group"]
        waiting_time = config["waiting_time"]
        device = config["device"]
        params = config["params"]
        #-------- GET PREV DATA -----------
        for field in groupby:            
            if field in message:
                key = message[field]
            else:
                key = "-"
            if key in data:
                print(data[key])
                data = data[key]
            else:
                data = {}

        if len(data) == 0 :
            data = {
                "prev_time":0,
                "prev_data":[],
                "prev_filter":[]
            }
        last_time = data["prev_time"]
        last_data = data["prev_data"]
        last_filter_data = data["prev_filter"]
        #-----------------------------------
        ctime = message["date_add_server"] / 1000
        if(ctime-last_time > waiting_time):
            last_time = 0
            last_data = []
            last_filter_data = []
        
        #------------- METHOD --------------
        maxparams = 2
        if method == "bandpass":
            maxparams = 4
        if method == "kalmanbasic":
            maxparams = 1
            P = 0 #Error covariance
            K = 0 #Kalman Gain
            H = 1 #measurement map scalar
            if("H" in params):
                H = params["H"]
        
        value = 0
        field = config["field"]
        if(field in message):
            value = float(message[field])
            if not filterController.is_float(value) :
                value = 0 
        print(value, message[field], field)
        
        last_data.append(float(value))  
        if(len(last_data)>maxparams):
            filter_data = value                         
            try:
                if method == "lowpass":
                    print("-----------------------------------")
                    filter_data = filterController.scipy_low(params["cutoff"], ctime-last_time,
                                value, last_data[1], last_data[0],
                                last_filter_data[1], last_filter_data[0])

                elif method == "highpass":
                    filter_data = filterController.scipy_high(params["cutoff"], ctime-last_time,
                                value, last_data[1], last_data[0],
                                last_filter_data[1], last_filter_data[0])
                elif method == "bandpass":
                    filter_data = filterController.scipy_band_lfilter(params["low_cutoff"], params["high_cutoff"], ctime-last_time,
                                value, last_data[3], last_data[2], last_data[1], last_data[0],
                                last_filter_data[3], last_filter_data[2], last_filter_data[1], last_filter_data[0])
                elif method == "kalmanbasic":
                    filter_data,K,P = filterController.basic_kalman(value,params["R"],H,K,params["Q"],P,last_data[0])
            except:

                filter_data = value 

            filter_data = float("{:.2f}".format(filter_data))   
            print(filter_data)
            last_filter_data.append(float(filter_data))                
            del last_data[0]
            del last_filter_data[0]            
        else:
            last_filter_data.append(float(value))
            filter_data = float(value)
        last_time = ctime
        #-------- SAVE DATA --------------
        collection = "sensor_data_"+device
        queryUpdate = {
            "_id":ObjectId(message["_id"])
        }
        updateData = {
            field+"_filter":filter_data
        }
        deviceController.updateSensorData(collection,queryUpdate,updateData)
        data = {
            "prev_time":last_time,
            "prev_data":last_data,
            "prev_filter":last_filter_data
        }
        group = groupby[:]
        filecon["data"] = data_rec(group,message,filecon["data"],data)
        update_config(code,filecon)
        #-------- SAVE CONFIG ------------

def data_rec(group,message,current,new):
    field = group[0]
    del group[0]
    if field in message:
        key = message[field]
    else:
        key = "-"
    if key not in current:
        current[key] = {}
    if(len(group)>0):
        current[key] = data_rec(group,message,current,new)
    else:
        current[key] = new
    return current


def run_filter_service(item):
    topic_subs = "mqtt/output/device-"+item["device"]
    code = item["filter_code"]
    if( topic_subs not in topic_list ):
        topic_list[topic_subs] = []
        client.subscribe(topic_subs)
        print("Subscribe : "+topic_subs)
    if( code not in topic_list[topic_subs] ):
        topic_list[topic_subs].append(code)
    
    data = get_config(code)
    stat = "Update"
    if(data == False):
        data = {
            "config":{},
            "data":{}
        }
        stat = "Add"
    
    data["config"]={
        "stream":item["stream"],
        "device":item["device"],
        "field":item["field"],
        "waiting_time":item["waiting_time"],
        "method":item["method"]["name"],
        "params":item["method"]["parameter"],
        "group":item["group_data"]
    }
    update_config(code,data)
    print(stat+" Filter code "+code+": Device "+item["device"]+" - "+item["field"]+" - "+item["method"]["name"])

def get_config(file):
    if os.path.isfile(config_folder+"/"+file+'.json'):
        with open(config_folder+"/"+file+'.json') as outfile:
            return json.load(outfile)
    else:
        return False
    
def update_config(file,data):
    json_string = json.dumps(data)
    with open(config_folder+"/"+file+'.json', 'w') as outfile:
        outfile.write(json_string)

def remove_config(file):
    if os.path.exists(config_folder+"/"+file+'.json'):
        os.remove(config_folder+"/"+file+'.json')
        return True
    else:
        return False  

def filter_list():
    query = {
        "stream":True
    }
    result = filterController.find(query)
    if result['status']:
        for val in result['data']:
            run_filter_service(val)
        print(topic_list)

if __name__ == "__main__":
    client = mqttClient.Client("Python3"+cloud9Lib.randomOnlyString(4))               
    # client.username_pw_set(username=user, password=password)    #set username and password
    client.on_connect= on_connect                      
    client.on_message= on_message                      
    client.connect(broker_address, port=port)          
    client.loop_start()  
    try:
        while True:
            time.sleep(1)
    
    except KeyboardInterrupt:
        print("exiting")
        client.disconnect()
        client.loop_stop()

    





# {
#     "config":{
#         "method":"-"
#         "parameters":{
#             "---":
#             "---":
#         },
#         "waiting_time":...,
#         "group":[...,...]
#     },
#     "data":{
#         "prev_time":.....[unix]
#         "prev_data":[]
#         "prev_filter":[]
#     }
# }
# "mqtt/output/device-[device_code]"
