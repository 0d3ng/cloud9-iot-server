#!/usr/bin/python3
import json, sys, base64
from function import *
from controller import comChannelController
from controller import commETLController
from controller import commLogController
from pytz import timezone
from datetime import datetime
from kafka import KafkaConsumer
from json import loads


broker_address = "103.106.72.187" #"161.117.58.227"
port = "9092"
group_id = 'SEMAR-IoT-Platform2'
topic_list = {}
main_folder = 'data'
topic_active = ['kafka-service-subscribe','kafka-service-unsubscribe']

consumer = KafkaConsumer(   
    bootstrap_servers=[broker_address+':'+port],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=group_id,
    value_deserializer=lambda x: loads(x.decode('utf-8')))
consumer.subscribe(topic_active)
# print(topic_active)
sys.stdout.flush()
def subscribe_list():
    query = {
        "active":True,
        "channel_type": "kafka"
    }
    result = comChannelController.find(query)
    for val in result['data']:
        topics = str(val['topic'])
        topic_list[topics] = val['channel_code']        
        topic_active.append(topics)
        print("Subscribe Topic: "+topics)
        # sys.stdout.flush()
    print(topic_active)
    consumer.subscribe(topic_active)

def message_handler(topic,msgdata):
    raw_object = msgdata
    print("Topic: "+topic)
    # print("Message: ")
    # print(msgdata)
    # sys.stdout.flush()
    message_insert(topic,raw_object,msgdata)

def message_handler_subs(message):
    topic = message['topic']
    channel_code = message['channel_code']
    topic_list[topic] = channel_code
    topic_active.append(topic)
    print(topic_active)
    sys.stdout.flush()
    consumer.subscribe(topic_active)
    print("Subscribe Topic: "+topic)
    # sys.stdout.flush()


def message_handler_unsub(message):
    topic = message['topic']
    try:
        print("Unsubscribe Topic: "+topic)
        # sys.stdout.flush()
        topic_active.remove(topic)
        print(topic_active)
        # sys.stdout.flush()
        consumer.subscribe(topic_active)
        del topic_list[topic]
    except KeyError:
        pass


#INI BELUM DIEDIT#
def message_insert(topic,message,messageStr):
    #Log Insert#
    print("Topic: "+topic)
    # sys.stdout.flush()
    insertLog = {
        'topic' : topic,
        'channel_type':'kafka',
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
        response = {"status":False, "message":"Wrong communication topic",'data':json.loads(message)} 
        insertLog['response'] = response
        commLogController.add(insertLog);
    else:
        channelData = resultChannel['data']

    infoKafka = {
        'topic' : topic,
        'channel_type':'kafka',
    }
    if 'date_add' in message :
        try:
            if(isinstance(message['date_add'],int)):
                infoKafka['date_add_sensor_unix'] = message['date_add']
                try:
                    today = datetime.fromtimestamp(round(message['date_add']),timezone('Asia/Jakarta')) #datetime.fromtimestamp(round(message['date_add']))
                except:
                    today = datetime.fromtimestamp(round(message['date_add']/1000),timezone('Asia/Jakarta')) #datetime.fromtimestamp(round(message['date_add']/1000))
                infoKafka['date_add_sensor'] = today
            else:
                infoKafka['date_add_sensor'] = datetime.strptime(message['date_add'],'%Y-%m-%d %H:%M:%S')
        except:
            infoKafka['date_add_sensor'] = message['date_add']
    else :
        infoKafka['date_add_sensor'] = None

    if 'device_code' in message :
        insert = commETLController.etl(channelData['collection_name'],channelData['index_log'],infoKafka,message['device_code'],message)
    else :
        insert = commETLController.nonetl(channelData['collection_name'],channelData['index_log'],infoKafka,message)
    
    if not insert['status']:
        response = {"status":False, "message":"Failed to add", 'data':json.loads(self.request.body)}               
    else:
        response = {'message':'Success','status':True}    
    insertLog['response'] = response
    commLogController.add(insertLog);

def test():
    consumer.subscribe(['message-sensor-2k93k2'])
    # consumer.subscribe(['message-sensor-ghfr0k'])

subscribe_list()
for msg in consumer:
    print(msg.topic)
    topic = msg.topic
    jsonMsg = msg.value
    if topic == 'kafka-service-subscribe':
        message_handler_subs(jsonMsg)
    elif topic == 'kafka-service-unsubscribe':
        message_handler_unsub(jsonMsg)
    elif topic in topic_list:
        message_handler(topic,jsonMsg)



