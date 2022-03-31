# import serial
import paho.mqtt.client as mqtt
import time
from datetime import datetime
from pytz import timezone

# ser = serial.Serial('/dev/ttyUSB0', 115200, timeout=1)
# set broker address and port
# broker_address = "blynk.ekokepet.com"
broker_address = "localhost" #"103.106.72.188" #"broker.emqx.io" #"broker.hivemq.com" #"test.mosquitto.org"
port = 1883
# set topic publish and subscribe
pubtop = "/Project_IPS/client3" #originally client3 for RC2
subtop = "/Project_IPS/serv"
FLAG = True
import csv

def writeCSV(file,value):    
    with open("log/log_"+file+".csv",'a+', newline='') as f:
        f.writelines(value+"\n")

# time.sleep(60) #waiting for internet connection
# connect to broker
client = mqtt.Client()
client.connect(broker_address, port)
client.loop_start()
client.subscribe(subtop)

while True:
    # # send string data to server
    # ser_bytes = ser.readline().decode(
    #     'utf-8')[:-1].rstrip()  # Read the newest output
    # if ser_bytes != None:
    #     if ser_bytes != "\x00":
    #         # Send string data to server
    #         stringData = ser_bytes
    #         client.publish(pubtop, stringData)
    #         print("client-3:data sent"+stringData) #client-3
    #     else:
    #         print("data empty")
    #         pass
    stringData = "---"
    now = datetime.now(timezone('Asia/Tokyo'))
    cDate = now.strftime('%Y-%m-%d')
    cTime = now.strftime('%H:%M:%S')
    writeCSV(cDate,pubtop+","+cDate+","+cTime+","+stringData)
    print("client-3:data sent"+stringData)
    time.sleep(0.5)


# Terlampir program di receivers.
# R1 (D208)  172:28:235:19
# R2 (D207)  172.28.235.47
# R3 (RC2)   172:28:235:21
# R4 (D307)  172:28:235:26
# R5 (D306)  172:28:235:28
# R6 (Corr3)  172:28:235:20