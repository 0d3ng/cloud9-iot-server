import multiprocessing
import sys, json, time
import paho.mqtt.client as mqttClient #Must Install Req
sys.path.append('../../')
from function import cloud9Lib
from datetime import datetime,timedelta
from configparser import ConfigParser
config = ConfigParser()
config.read("config.ini")
#Config
comm_list = {} 
comm_subs = {}
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
        # subscribe_list()             
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
    print("Start Service : ",combi_code)
    sys.stdout.flush()
    comm_subs[combi_code] = multiprocessing.Process(target=worker, args=(combi_code,time_loop))
    comm_subs[combi_code].start()

def on_message_unsubscribe(message):
    combi_code = message['combi_code']
    print("Stop Service : ",combi_code)
    sys.stdout.flush()
    comm_subs[combi_code].terminate()
    comm_subs[combi_code].join()
    del comm_subs[combi_code]

def worker(code, time_loop):
    next_time = datetime.now() + timedelta(minutes=int(time_loop))
    next_time = next_time.strftime('%Y-%m-%d %H:%M')
    print("next ",next_time)
    while True:
        curentTime = datetime.now().strftime('%Y-%m-%d %H:%M')
        if(next_time == curentTime):
            next_time = datetime.now() + timedelta(minutes=int(time_loop))
            next_time = next_time.strftime('%Y-%m-%d %H:%M')
            print("Process for ",code,"  -> next ",next_time)
            print()

if __name__ == "__main__":    
    client = mqttClient.Client("Python3-Combi_"+cloud9Lib.randomOnlyString(4))               
    # client.username_pw_set(username=user, password=password)    #set username and password
    client.on_connect= on_connect                      
    client.on_message= on_message                      
    client.connect(broker_address, port=port)          
    client.loop_start()        
    
    while Connected != True:    #Wait for connection
        time.sleep(0.1)
    
    client.subscribe(config["MQTT"]["combi_stream_start"])
    client.subscribe(config["MQTT"]["combi_stream_stop"])

    try:
        while True:
            time.sleep(1)
    
    except KeyboardInterrupt:
        print("exiting")
        client.disconnect()
        client.loop_stop()


# def worker(dict, key, item):
#     dict[key] = item

# if __name__ == '__main__':
#     mgr = multiprocessing.Manager()
#     dictionary = mgr.dict()

#     jobs = [multiprocessing.Process(target=worker, args=(dictionary, i, i*2)) \
#     for i in range(10)]

#     for j in jobs:
#         j.start()

#     for j in jobs:
#         j.join()

#     print(dictionary)

