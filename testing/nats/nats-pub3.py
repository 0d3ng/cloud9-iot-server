#!/usr/bin/python3
from pynats import NATSClient

subject = 'message/sensor/py787b'
msg = "test"
if __name__ == '__main__':
    client = NATSClient("nats://127.0.0.1:4222",socket_timeout=2, verbose=True)
    client.connect()
    #client.ping()
    client.publish(subject, payload=msg)
    client.close()
