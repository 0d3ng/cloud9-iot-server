import multiprocessing
import sys, json, time
import paho.mqtt.client as mqttClient #Must Install Req
from function import *
from controller import datasyncController
from datetime import datetime,timedelta
from configparser import ConfigParser
from importlib import reload
from pytz import timezone
config = ConfigParser()
config.read("config.ini")
#Config
datasync_list = {} 
datasync_subs = {}
Connected = False
broker_address= config["MQTT"]["broker"]
port = int(config["MQTT"]["port"])


def on_connect(client, userdata, flags, rc):
    print("rc: "+str(rc))
    sys.stdout.flush()
    if rc == 0:
        print("Connected to broker")
        print("------------------------------------")
        sys.stdout.flush()
        global Connected                
        Connected = True   
        stream_list()             
    else:
        print("Connection failed")
        sys.stdout.flush()
 
def on_message(client, userdata, message):
    raw_msg = message.payload.decode("utf-8")
    try:
        raw_object = json.loads(raw_msg)
    except:
        raw_object = {"failed":True}
    if message.topic == config["MQTT"]["datasync_stream_start"] :
        on_message_subscribe(raw_object)
    elif message.topic == config["MQTT"]["datasync_stream_start"] :
        on_message_unsubscribe(raw_object)
    else :
        print("-----------------")
      
def on_message_subscribe(message):
    datasync_code = message['datasync_code']
    time_loop = message['time_loop']
    if datasync_code not in datasync_subs:    
        datasync_subs[datasync_code] = multiprocessing.Process(target=worker, args=(datasync_code,time_loop))
        datasync_subs[datasync_code].start()

def on_message_unsubscribe(message):
    datasync_code = message['datasync_code']
    print("Stop Service : ",datasync_code)
    sys.stdout.flush()
    if datasync_code in datasync_subs:
        datasync_subs[datasync_code].terminate()
        datasync_subs[datasync_code].join()
        del datasync_subs[datasync_code]

def worker(code, time_loop):
    reload(db)
    reload(datasyncController)
    last_time = datetime.now(timezone('Asia/Tokyo')).strftime('%Y-%m-%d %H:%M:00')
    next_time = datetime.now(timezone('Asia/Tokyo')) + timedelta(seconds=int(time_loop))
    next_time = next_time.strftime('%Y-%m-%d %H:%M:%S')
    print("Start Service : ",code)
    sys.stdout.flush()
    while True:
        curentTime = datetime.now(timezone('Asia/Tokyo')).strftime('%Y-%m-%d %H:%M:%S')
        if(next_time == curentTime):
            try:
                query = {"datasync_code":code}
                dataSyncData = datasyncController.findOne(query)
                if dataSyncData['status']:    
                    dataSyncData = dataSyncData["data"]            
                    # print("Process for ",code," ",last_time," ",next_time)
                    next_time = datetime.strptime(next_time,'%Y-%m-%d %H:%M:%S') #To get second data.
                    next_time = next_time.strftime('%Y-%m-%d %H:%M:%S')
                    try:
                        item = datasyncController.datasyncProcess(dataSyncData["schema_code"],dataSyncData["field"],last_time,next_time,"",True)
                    except:
                        print("------++++++------")
                        print(code)
                        print(next_time)
                        print("Error")
                        print("------------------")
                        sys.stdout.flush()
                    # print("Total Insert ",code," : ",item," --> ",last_time," ",next_time)                
                    #Tambahkan Funsgi untuk mengirimkan hasil kombinasi ke sebagai MQTT Message.
                    time_loop = dataSyncData["time_loop"]
                next_time = datetime.now(timezone('Asia/Tokyo')) + timedelta(seconds=int(time_loop))
                next_time = next_time.strftime('%Y-%m-%d %H:%M:%S')
                last_time = curentTime
                # print(code)
                # print(next_time)
                # sys.stdout.flush()
            except:
                continue

def stream_list():
    query = {
        "stream":True
    }
    result = datasyncController.find(query)
    if result['status']:        
        for val in result['data']:
            datasync_code = val['datasync_code']
            time_loop = val['time_loop']
            datasync_subs[datasync_code] = multiprocessing.Process(target=worker, args=(datasync_code,time_loop))
            datasync_subs[datasync_code].start() 


if __name__ == "__main__":    
    client = mqttClient.Client("Python3-DataSync_"+cloud9Lib.randomOnlyString(4))               
    # client.username_pw_set(username=user, password=password)    #set username and password
    client.on_connect= on_connect                      
    client.on_message= on_message                      
    client.connect(broker_address, port=port)          
    client.loop_start()        
    
    while Connected != True:    #Wait for connection
        time.sleep(0.1)
    
    client.subscribe(config["MQTT"]["datasync_stream_start"])
    client.subscribe(config["MQTT"]["datasync_stream_start"])

    try:
        while True:
            time.sleep(1)
    
    except KeyboardInterrupt:
        print("exiting")
        client.disconnect()
        client.loop_stop()


