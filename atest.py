import time
import paho.mqtt.client as paho
broker="103.106.72.188"
topic="/Project_IPS/client61"
port=1883
#define callback
def on_message(client, userdata, message):
    time.sleep(1)
    print("received message =",str(message.payload.decode("utf-8")))

def on_connect(self,client, userdata, flags, rc):
    print("rc: "+str(rc))
    if rc == 0:
        print(topic)
        client.subscribe(topic)
    else:
        print("Connection failed")

client= paho.Client("iu9u9") #create client object client1.on_publish = on_publish #assign function to callback client1.connect(broker,port) #establish connection client1.publish("house/bulb1","on")
######Bind function to callback
client.on_message=on_message
client.on_connect=on_connect
#####
print("connecting to broker ",broker)
client.connect(broker,port=port)#connect
client.loop_start() #start loop to process received messages