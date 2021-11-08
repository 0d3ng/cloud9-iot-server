#!/usr/bin/python3
from pynats import NATSClient
import argparse
import json
import time
import random
from datetime import datetime
import sys

broker = "103.106.72.187"#"127.0.0.1"
port = "4222"
subject = 'message/sensor/2k93k2'

client = NATSClient("nats://"+broker+":"+port,socket_timeout=2, verbose=True)
client.connect()
for x in range(1000):
    today = datetime.today()
    msg = {
        "device_code":"2k93k2-hf83",
        "date_add":round(datetime.today().timestamp() * 1000), #today.strftime("%Y-%m-%d %H:%M:%S"),
        "gps":{
            "latitude":-7.575973 + (random.randint(1,1000) / 10000 ),
            "longitude":112.878304 - ( random.randint(1,1000) / 10000 )
        },
        "temperature": random.randint(2000,3500) / 100,
        "fuel":900
    }
    msg = json.dumps(msg)
    print(msg)
    sys.stdout.flush()
    client.publish(subject, payload=msg)
    time.sleep(1)

client.close()
