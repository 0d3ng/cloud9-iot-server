import paho.mqtt.client as mqtt
import json
from datetime import datetime
from datetime import timezone as timezone2
from pytz import timezone

broker = '103.106.72.181'#'localhost'
topic = "edge/update/dc312b34" #"/simulationIPS/client2/response"
device_code = "nu63"

def writeLog(file,value):
    with open("log/log_"+file+".log",'a+',newline='') as f:
        f.write(value+'\n')

def on_connect(client, userdata, flags, rc):  # The callback for when the client connects to the broker
    print("Connected with result code {0}".format(str(rc)))  # Print result of connection attempt
    client.subscribe(topic)  # Subscribe to the topic “digitest/test1”, receive any messages published on it


def on_message(client, userdata, message):  # The callback for when a PUBLISH message is received from the server.
    print(message.payload.decode("utf-8"))
    # now = datetime.now()
    # cDate = now.strftime("%Y-%m-%d")
    # cTime = now.strftime("%H:%M:%S")
    # cUnix = int(now.timestamp() * 1000)
    # raw_msg = message.payload.decode("utf-8")
    # writeLog(broker+"_"+device_code+"_"+cDate,cDate+","+cTime+","+str(cUnix)+","+raw_msg)


client = mqtt.Client("digi_mqtt_test")  # Create instance of client with client ID “digi_mqtt_test”
client.on_connect = on_connect  # Define callback function for successful connection
client.on_message = on_message  # Define callback function for receipt of a message
# client.connect("m2m.eclipse.org", 1883, 60)  # Connect to (broker, port, keepalive-time)
client.connect(broker, 1883)
client.loop_forever()  # Start networking daemon
