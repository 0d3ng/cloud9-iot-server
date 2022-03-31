import sys, json, time
import paho.mqtt.client as mqttClient #Must Install Req
from function import *
from controller import comChannelController
from controller import commETLController
from controller import commLogController
from datetime import datetime
from datetime import timezone as timezone2
from pytz import timezone
from configparser import ConfigParser
import csv
config = ConfigParser()
config.read("config.ini")
#Config
comm_list = {} 
comm_subs = {}
Connected = False
broker_address= config["MQTT"]["broker"]
port = int(config["MQTT"]["port"])


class Comm:
    def __init__(self, code, broker, port, topic, device_code, collection, index_log):
        self.code = code
        self.broker = broker
        self.port = int(port)
        self.topic = topic
        self.device_code = device_code
        self.collection = collection
        self.index_log = index_log
        self.Conected = False
        
    
    def on_connect(self,client, userdata, flags, rc):
        print("rc: "+str(rc))
        sys.stdout.flush()
        if rc == 0:
            print("Connected to broker:"+self.broker+":"+str(self.port))
            sys.stdout.flush()
            self.Conected=True  
            self.client.subscribe(self.topic)
            print("Connected to topic:"+self.topic)
            print("-------------------------------")
            sys.stdout.flush()
        else:
            print("Connection failed")
            sys.stdout.flush()
    
    def on_message(self,client, userdata, message):
        hack = False
        receive_unix_time2 = round(datetime.now(timezone('Asia/Tokyo')).timestamp()*1000)
        receive_unix_time = round(datetime.now(timezone2.utc).timestamp()*1000)
        raw_msg = message.payload.decode("utf-8")
        if "ts" not in raw_msg and "::t" not in raw_msg:
            cDate = datetime.now(timezone('Asia/Tokyo')).strftime("%Y-%m-%d")
            cTime = datetime.now(timezone('Asia/Tokyo')).strftime("%H:%M:%S")
            writeLog(self.broker+"_"+self.device_code+"_"+cDate,cDate+","+cTime+","+str(receive_unix_time2)+","+raw_msg)
        # self.client.publish(self.topic+"/feedback",payload=raw_msg)
        # print(self.device_code)
        # print("--RAW MESSAGE---")
        # print(raw_msg)
        insertLog = {
            'topic' : message.topic,
            'channel_type':'mqtt',
            'raw_message' : message.payload.decode("utf-8")
        } 
        infoMqtt = insertLog.copy()
        try:
            raw_object = json.loads(raw_msg)
            # insertLog['raw_message'] = raw_object
        except:
            raw_msg = raw_msg.replace('\u0000', '')
            raw_object = cloud9Lib.delimeterExtract(raw_msg)
            # insertLog['raw_message'] = raw_msg
        # print("--RAW OBJECT---")
        # print(raw_object)
        # print("-------------------------------------------")
        # sys.stdout.flush()        
        if ('ts' in raw_object) and (len(raw_object) == 1) :
            hack = True        
        if hack == False:            
            message_obj = raw_object
            insert = commETLController.etl(self.collection,self.index_log,infoMqtt,self.device_code,message_obj,receive_unix_time)
            if not insert['status']:
                response = {"status":False, "message":"Failed to add", 'data':raw_msg}               
            else:
                response = {'message':'Success','status':True}   
            insertLog['response'] = response
            commLogController.add(insertLog)   

    def connect(self):
        self.client = mqttClient.Client(self.code+cloud9Lib.randomOnlyString(4))
        self.client.on_connect=self.on_connect
        self.client.on_message=self.on_message
        try:
            self.client.connect(self.broker,self.port) #connect to broker
            self.client.loop_start()
            print("Connecting to broker:"+self.broker+":"+str(self.port))
            sys.stdout.flush()
        except:
            print(self.broker+": connection failed")
            self.client.loop_stop()

    def disconnect(self):
        self.client.loop_stop()    #Stop loop
        self.client.disconnect() # disconnect
        print("Diconnecting from:"+self.topic+"@"+self.broker+":"+str(self.port))
        print("-------------------------------------------")
        sys.stdout.flush()

def writeLog(file,value):
    with open("log/log_"+file+".log",'a+',newline='') as f:
        f.write(value+'\n')

def subscribe_list():
    query = {
        "active":True,
        "channel_type": "mqtt",
        "server":{ "$nin": [ None, "" ] },
        "port":{ "$nin": [ None, "" ] },
        "topic":{ "$nin": [ None, "" ] }
    }
    result = comChannelController.find(query)
    if result['status']:        
        for val in result['data']:
            comm_list[val['channel_code']] = {
                'server':val['server'],
                'port':val['port'],
                'topic':val['topic'],
                'collection_name':val['collection_name'],
                'device_code':val['device_code']
            }
            comm_subs[val['channel_code']] = Comm(val['channel_code'],val['server'],val['port'],val['topic'],val['device_code'],val['collection_name'],val['index_log'])
            comm_subs[val['channel_code']].connect()

def on_connect(client, userdata, flags, rc):
    print("rc: "+str(rc))
    sys.stdout.flush()
    if rc == 0:
        print("Connected to broker")
        print("------------------------------------")
        sys.stdout.flush()
        global Connected                
        Connected = True   
        subscribe_list()             
    else:
        print("Connection failed")
        sys.stdout.flush()
 
def on_message(client, userdata, message):
    raw_msg = message.payload.decode("utf-8")
    try:
        raw_object = json.loads(raw_msg)
    except:
        raw_object = {"failed":True}
    if message.topic == config["MQTT"]["other_subscribe"] :
        on_message_subscribe(raw_object)
    elif message.topic == config["MQTT"]["other_unsubscribe"] :
        on_message_unsubscribe(raw_object)
    else :
        # message_insert(message.topic,raw_object,raw_msg)
        print("undefined messages")
      
def on_message_subscribe(message):
    print(message)
    channel_code = message['channel_code']
    if(channel_code):
        query = {
            "channel_code": channel_code
        }
        result = comChannelController.findOne(query)
        if result['status']: 
            val = result['data']
            print("New Subscribe to: "+val['topic']+"@"+val['server']+":"+str(val['port']))
            if(val['channel_code'] in comm_subs):
                try:
                    comm_subs[val['channel_code']].disconnect()
                    del comm_subs[val['channel_code']]
                except KeyError:
                    pass
            comm_subs[val['channel_code']] = Comm(val['channel_code'],val['server'],val['port'],val['topic'],val['device_code'],val['collection_name'],val['index_log'])
            comm_subs[val['channel_code']].connect()

def on_message_unsubscribe(message):
    print(message)
    channel_code = message['channel_code']
    try:
        comm_subs[channel_code].disconnect()
        del comm_subs[channel_code]
    except KeyError:
        pass

client = mqttClient.Client("Python3-Other_"+cloud9Lib.randomOnlyString(4))               
# client.username_pw_set(username=user, password=password)    #set username and password
client.on_connect= on_connect                      
client.on_message= on_message                      
client.connect(broker_address, port=port)          
client.loop_start()        
 
while Connected != True:    #Wait for connection
    time.sleep(0.1)
 
client.subscribe(config["MQTT"]["other_subscribe"])
client.subscribe(config["MQTT"]["other_unsubscribe"])

try:
    while True:
        time.sleep(1)
 
except KeyboardInterrupt:
    print("exiting")
    client.disconnect()
    client.loop_stop()