#!/usr/bin/python3
from pynats import NATSClient
import argparse
import json
import time
import random
from datetime import datetime
import sys


subject = 'message/sensor/py787b'


client = NATSClient("nats://127.0.0.1:4222",socket_timeout=2, verbose=True)
client.connect()
for x in range(1000):
    today = datetime.today()
    msg = {
        "device_code":"py787b-mw47",
        "date_add":today.strftime("%Y-%m-%d %H:%M:%S"),
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
    time.sleep(5)

client.close()
