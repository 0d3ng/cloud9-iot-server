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
        method = config["method"]
        data = filecon["data"]
        #------------- METHOD --------------
        last_time = data["prev_time"]
        last_data = data["prev_data"]
        last_filter_data = data["prev_filter"]
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
            "data":{
                "prev_time":0,
                "prev_data":[],
                "prev_filter":[]
            }
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
