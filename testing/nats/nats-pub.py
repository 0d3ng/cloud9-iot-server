#!/usr/bin/python3
from pynats import NATSClient
import argparse
import json

subject = 'message/sensor/92868a'
msg = {
    "device_code":"92868a-vr88",
    "date_add":"2020-06-07 10:10:10",
    "gyro":{
        "x":11.1,
        "y":12.4,
        "z":15
    },
    "proximity":111.5,
    "rssi":10.5
}
msg = json.dumps(msg)
if __name__ == '__main__':
    client = NATSClient("nats://103.56.148.215:4222",socket_timeout=2, verbose=True)
    client.connect()
    #client.ping()
    client.publish(subject, payload=msg)
    client.close()
