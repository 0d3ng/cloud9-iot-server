#!/usr/bin/python3

from pynats import NATSClient #Must Install Req
from datetime import datetime
import json, sys
from function import *
from controller import comChannelController
from controller import commETLController
from controller import commLogController
from pytz import timezone
#

broker_address = "103.56.148.215" #"161.117.58.227"
port = "4222"
topic_list = {}
topicsdata = {} 

client = NATSClient("nats://"+broker_address+":"+port,verbose=True)
def message_handler(data):
    topic = data.subject
    msgdata = data.payload.decode("utf-8")
    try:
        raw_object = json.loads(msgdata)
    except:
        raw_object = {"failed":True}
    print("Topic: "+topic)
    print("Message: "+msgdata)
    sys.stdout.flush()
    message_insert(topic,raw_object,msgdata)


def message_handler_subs(data):
    message = json.loads(data.payload.decode("utf-8"))
    topic = message['topic']
    channel_code = message['channel_code']
    topic_list[topic] = channel_code
    topicsdata[topic] = client.subscribe(topic, callback=message_handler)
    print("Subscribe Topic: "+topic)
    sys.stdout.flush()


def message_handler_unsub(data):
    message = json.loads(data.payload.decode("utf-8"))
    topic = message['topic']
    try:
        client.unsubscribe(topicsdata[topic])
        print("Unsubscribe Topic: "+topic)
        sys.stdout.flush()
        del topic_list[topic]
    except KeyError:
        pass

def subscribe_list():
    query = {
        "active":True,
        "channel_type": "nats"
    }
    result = comChannelController.find(query)
    for val in result['data']:
        topic_list[val['topic']] = val['channel_code']
        topicsdata[val['topic']] = client.subscribe(val['topic'], callback=message_handler)
        print("Subscribe Topic: "+val['topic'])
        sys.stdout.flush()

def message_insert(topic,message,messageStr):
    #Log Insert#
    print("Topic: "+topic)
    sys.stdout.flush()
    insertLog = {
        'topic' : topic,
        'channel_type':'nats',
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
        'channel_type':'nats',
    }
    if 'date_add' in message :
        try:
            if(isinstance(message['date_add'],int)):
                infoMqtt['date_add_sensor_unix'] = message['date_add']
                try:
                    today = datetime.fromtimestamp(round(message['date_add']),timezone('Asia/Jakarta')) #datetime.fromtimestamp(round(message['date_add']))
                except:
                    today = datetime.fromtimestamp(round(message['date_add']/1000),timezone('Asia/Jakarta')) #datetime.fromtimestamp(round(message['date_add']/1000))
                infoMqtt['date_add_sensor'] = today
            else:
                infoMqtt['date_add_sensor'] = datetime.strptime(message['date_add'],'%Y-%m-%d %H:%M:%S')
        except:
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

client.connect()
client.subscribe("nats/service/subscribe",callback=message_handler_subs)
client.subscribe("nats/service/unsubscribe",callback=message_handler_unsub)
subscribe_list()
try :
    client.wait()
except KeyboardInterrupt:
    print("exiting")
    client.close()