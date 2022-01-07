import multiprocessing
import sys, json, time
import paho.mqtt.client as mqttClient #Must Install Req
sys.path.append('../../')
from function import cloud9Lib
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
        # combi_service_list()             
    else:
        print("Connection failed")
        sys.stdout.flush()

# Called when the broker responds to a subscribe request.
def on_subscribe(client, userdata, mid, granted_qos):
    print("Subscribed:", str(mid), str(granted_qos))

def on_message_subscribe(message):
    channel_code = message['combi_code']
    print("Start Function : ",str(channel_code))

def on_message_unsubscribe(message):
    channel_code = message['combi_code']
    print("Stop Function : ",str(channel_code))

client = mqttClient.Client("Python3-Combi_"+cloud9Lib.randomOnlyString(4))               
# client.username_pw_set(username=user, password=password)    #set username and password
client.on_connect= on_connect                      
# client.on_message= on_message                      
client.connect(broker_address, port=port)
client.on_subscribe = on_subscribe          
client.loop_start()        
 
while Connected != True:    #Wait for connection
    time.sleep(0.1)
 
client.subscribe(config["MQTT"]["combi_stream_master"]+"#")
client.message_callback_add(config["MQTT"]["combi_stream_start"], on_message_subscribe)
client.message_callback_add(config["MQTT"]["combi_stream_stop"], on_message_unsubscribe)

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

