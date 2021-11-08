import sys, json, time
import paho.mqtt.client as mqttClient #Must Install Req
from function import *
from controller import comChannelController
from controller import commETLController
from controller import commLogController
from datetime import datetime
from pytz import timezone

topic_list = {} 
Connected = False
broker_address= "localhost" #"161.117.58.227"
port = 1883                         
user = "OGRhNTI5MzE1YjY0ZWRlN2EwNjI2Mzg1"
password = "hdMFWDGTnfbhfoxoW7YXU8IwyAhFbD"


def on_connect(client, userdata, flags, rc):
    print("rc: "+str(rc))
    sys.stdout.flush()
    if rc == 0:
        print("Connected to broker")
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
    if message.topic == 'mqtt/service/subscribe' :
        on_message_subscribe(raw_object)
    elif message.topic == 'mqtt/service/unsubscribe' :
        on_message_unsubscribe(raw_object)
    else :
        message_insert(message.topic,raw_object,raw_msg)
    
    
def on_message_subscribe(message):
    topic = message['topic']
    channel_code = message['channel_code']
    topic_list[topic] = channel_code
    client.subscribe(topic)
    print("Subscribe Topic: "+topic)

def on_message_unsubscribe(message):
    topic = message['topic']
    channel_code = message['channel_code']
    topic_list[topic] = channel_code
    client.unsubscribe(topic)
    print("Unsubscribe Topic: "+topic)
    try:
        del topic_list[topic]
    except KeyError:
        pass

def subscribe_list():
    query = {
        "active":True,
        "channel_type": "mqtt"
    }
    result = comChannelController.find(query)
    if result['status']:        
        for val in result['data']:
            topic_list[val['topic']] = val['channel_code']
            client.subscribe(val['topic'])
            print("Subscribe Topic: "+val['topic'])
            sys.stdout.flush()
 

def message_insert(topic,message,messageStr):
    #Log Insert#
    print("Topic: "+topic)
    sys.stdout.flush()
    insertLog = {
        'topic' : topic,
        'channel_type':'mqtt',
    }
    if 'failed' in message :
        insertLog['raw_message'] = messageStr
    else:
        insertLog['raw_message'] = message
    #End Log Insert#
    queryChanel = {
        'channel_code' : topic_list[topic],
        'active': True 
    }
    resultChannel = comChannelController.findOne(queryChanel)
    if not resultChannel['status']:
        response = {"status":False, "message":"Wrong communication topic",'data':json.loads(self.request.body)} 
        insertLog['response'] = response
        commLogController.add(insertLog);
    else:
        channelData = resultChannel['data']

    infoMqtt = {
        'topic' : topic,
        'channel_type':'mqtt',
    }
    if 'date_add' in message :
        try:
            if(isinstance(message['date_add'],int)):
                infoMqtt['date_add_sensor_unix'] = message['date_add']
                try:
                    today = datetime.fromtimestamp(round(message['date_add']),timezone('Asia/tokyo')) #datetime.fromtimestamp(round(message['date_add']))
                except:
                    today = datetime.fromtimestamp(round(message['date_add']/1000),timezone('Asia/tokyo')) #datetime.fromtimestamp(round(message['date_add']/1000))
                infoMqtt['date_add_sensor'] = today
            else:
                infoMqtt['date_add_sensor'] = datetime.strptime(message['date_add'],'%Y-%m-%d %H:%M:%S')
        except:
            print("error")
            sys.stdout.flush()
            infoMqtt['date_add_sensor'] = message['date_add']
    else :
        infoMqtt['date_add_sensor'] = None

    if 'device_code' in message :
        insert = commETLController.etl(channelData['collection_name'],channelData['index_log'],infoMqtt,message['device_code'],message)
    else :
        insert = commETLController.nonetl(channelData['collection_name'],channelData['index_log'],infoMqtt,message)
    
    if not insert['status']:
        response = {"status":False, "message":"Failed to add", 'data':json.loads(self.request.body)}               
    else:
        response = {'message':'Success','status':True}    
    insertLog['response'] = response
    commLogController.add(insertLog);


client = mqttClient.Client("Python3")               
# client.username_pw_set(username=user, password=password)    #set username and password
client.on_connect= on_connect                      
client.on_message= on_message                      
client.connect(broker_address, port=port)          
client.loop_start()        
 
while Connected != True:    #Wait for connection
    time.sleep(0.1)
 
client.subscribe("mqtt/service/subscribe")
client.subscribe("mqtt/service/unsubscribe")

try:
    while True:
        time.sleep(1)
 
except KeyboardInterrupt:
    print("exiting")
    client.disconnect()
    client.loop_stop()