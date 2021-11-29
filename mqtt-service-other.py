import sys, json, time
import paho.mqtt.client as mqttClient #Must Install Req
from function import *
from controller import comChannelController
from controller import commETLController
from controller import commLogController
from datetime import datetime
from pytz import timezone

comm_list = {} 
comm_subs = {}

def subscribe_list():
    query = {
        "active":True,
        "channel_type": "mqtt",
        "server":{ "$nin": [ null, "" ] },
        "port":{ "$nin": [ null, "" ] },
        "topic":{ "$nin": [ null, "" ] }
    }
    result = comChannelController.find(query)
    if result['status']:        
        for val in result['data']:
            comm_list[val['token_access']] = val
            # client.subscribe(val['topic'])
            print("Subscribe Topic: "+val['topic'])
            sys.stdout.flush()