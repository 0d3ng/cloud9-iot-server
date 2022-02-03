import multiprocessing
import sys, json, time
import paho.mqtt.client as mqttClient #Must Install Req
from function import *
from method import *
from controller import combiController
from datetime import datetime,timedelta
from configparser import ConfigParser

config = ConfigParser()
config.read("config.ini")
#Config
comm_list = {}
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
    if message.topic == config["MQTT"]["combi_stream_start"] :
        on_message_subscribe(raw_object)
    elif message.topic == config["MQTT"]["combi_stream_stop"] :
        on_message_unsubscribe(raw_object)
    else :
        print("-----------------")
      
def on_message_subscribe(message):
    combi_code = message['combi_code']
    time_loop = message['time_loop']
    if combi_code not in comm_subs:    
        comm_subs[combi_code] = multiprocessing.Process(target=worker, args=(combi_code,time_loop))
        comm_subs[combi_code].start()

def on_message_unsubscribe(message):
    combi_code = message['combi_code']
    print("Stop Service : ",combi_code)
    sys.stdout.flush()
    if combi_code in comm_subs:
        comm_subs[combi_code].terminate()
        comm_subs[combi_code].join()
        del comm_subs[combi_code]

def worker(code, time_loop):
    last_time = datetime.now().strftime('%Y-%m-%d %H:%M')
    next_time = datetime.now() + timedelta(minutes=int(time_loop))
    next_time = next_time.strftime('%Y-%m-%d %H:%M')
    print("Start Service : ",code)
    sys.stdout.flush()
    while True:
        curentTime = datetime.now().strftime('%Y-%m-%d %H:%M')
        if(next_time == curentTime):
            query = {"combi_code":code}
            combiData = combiController.findOne(query)
            if combiData['status']:    
                combiData = combiData["data"]            
                # print("Process for ",code," ",last_time," ",next_time)
                next_time = datetime.strptime(next_time,'%Y-%m-%d %H:%M') - timedelta(minutes=1) #To get second data.
                next_time = next_time.strftime('%Y-%m-%d %H:%M')
                try:
                    item = combiController.combiProcess(combiData["schema_code"],combiData["field"],last_time,next_time,"",True)
                except:
                    print("------++++++------")
                    print(code)
                    print(next_time)
                    print("Error")
                    print("------------------")
                    sys.stdout.flush()
                # print("Totall Insert ",code," : ",item)                
                #Tambahkan Funsgi untuk mengirimkan hasil kombinasi ke sebagai MQTT Message.
                time_loop = combiData["time_loop"]
            next_time = datetime.now() + timedelta(minutes=int(time_loop))
            next_time = next_time.strftime('%Y-%m-%d %H:%M')
            last_time = curentTime
            # print(code)
            # print(next_time)
            # sys.stdout.flush()

def stream_list():
    query = {
        "stream":True
    }
    result = combiController.find(query)
    if result['status']:        
        for val in result['data']:
            combi_code = val['combi_code']
            time_loop = val['time_loop']
            comm_subs[combi_code] = multiprocessing.Process(target=worker, args=(combi_code,time_loop))
            comm_subs[combi_code].start()


if __name__ == "__main__":    
    client = mqttClient.Client("Python3-analytical_"+cloud9Lib.randomOnlyString(4))               
    # client.username_pw_set(username=user, password=password)    #set username and password
    client.on_connect= on_connect                      
    client.on_message= on_message                      
    client.connect(broker_address, port=port)          
    client.loop_start()        
    
    while Connected != True:    #Wait for connection
        time.sleep(0.1)
    
    client.subscribe(config["MQTT"]["analytical_stream_start"])
    client.subscribe(config["MQTT"]["analytical_stream_stop"])

    try:
        while True:
            time.sleep(1)
    
    except KeyboardInterrupt:
        print("exiting")
        client.disconnect()
        client.loop_stop()



