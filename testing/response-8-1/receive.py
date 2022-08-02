import paho.mqtt.client as mqttClient
import json
from datetime import datetime
from datetime import timezone as timezone2
from pytz import timezone
import random,string

broker="103.106.72.188"#"localhost"#
port=1883
# topic_l = ["/simulationIPS/client1/response","/simulationIPS/client2/response","/simulationIPS/client3/response","/simulationIPS/client4/response","/simulationIPS/client5/response","/simulationIPS/client6/response"
#             ,"/simulationIPS/client7/response","/simulationIPS/client8/response","/simulationIPS/client9/response","/simulationIPS/client10/response","/simulationIPS/client11/response","/simulationIPS/client12/response","/simulationIPS/client13/response","/simulationIPS/client14/response"
#             ,"/simulationIPS/client15/response","/simulationIPS/client16/response","/simulationIPS/client17/response","/simulationIPS/client18/response","/simulationIPS/client19/response","/simulationIPS/client20/response"]

topic_l = []
for i in range(35):
    topic_l.append("/simulationIPS/client"+str(i+1)+"/response")

device_code = "hl36"
device_num = 500
file_goal = str(device_num)+"_"+datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
comm_subs = {}

def randomString(stringLength=8):
    letters1 = string.ascii_lowercase
    letters2 = string.ascii_uppercase 
    letters3 = string.digits
    return ''.join(random.choice(letters1 + letters2 + letters3) for i in range(stringLength))

class Comm:
    def __init__(self, code, broker, port, topic):
        self.code = code
        self.broker = broker
        self.port = int(port)
        self.topic = topic
        
    def on_connect(self,client, userdata, flags, rc):
        print("rc: "+str(rc))
        if rc == 0:
            print("Connected to broker:"+self.broker+":"+str(self.port))
            self.Conected=True  
            self.client.subscribe(self.topic)
            print("Connected to topic:"+self.topic)
            print("-------------------------------")
        else:
            print("Connection failed")
    
    def on_message(self,client, userdata, message):
        now = datetime.now()
        cDate = now.strftime("%Y-%m-%d")
        cTime = now.strftime("%H:%M:%S")
        cUnix = int(now.timestamp() * 1000)
        raw_msg = message.payload.decode("utf-8")
        writeLog(file_goal,cDate+","+cTime+","+str(cUnix)+","+raw_msg)       

    def connect(self):
        self.client = mqttClient.Client(self.code+randomString(4))
        self.client.on_connect=self.on_connect
        self.client.on_message=self.on_message
        # try:
        self.client.connect(self.broker,self.port) #connect to broker
        self.client.loop_start()
        print("Connecting to broker:"+self.broker+":"+str(self.port))
        # except:
        #     print(self.broker+": connection failed")
        #     self.client.loop_stop()

def writeLog(file,value):
    with open("receive_"+file+".txt",'a+',newline='') as f:
        f.write('\n'+value)

def on_connect(client, userdata, flags, rc):  # The callback for when the client connects to the broker
    code = randomString(3)
    deviceid = [ [],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],
                [],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[] ]
    i = 0
    for x in range(device_num):
        deviceid[i].append(x+1)
        i+=1
        if i>34 :
            i = 0
    i = 0
    for topic in topic_l:
        listid = deviceid[i]  
        if len(listid)<1:
            continue
        i=i+1
        code = randomString(3)
        comm_subs[code] = Comm(code,broker,port,topic)
        comm_subs[code].connect()


def on_message(client, userdata, message):  # The callback for when a PUBLISH message is received from the server.
    now = datetime.now()
    cDate = now.strftime("%Y-%m-%d")
    cTime = now.strftime("%H:%M:%S")
    cUnix = int(now.timestamp() * 1000)
    raw_msg = message.payload.decode("utf-8")
    writeLog(file_goal,cDate+","+cTime+","+str(cUnix)+","+raw_msg)


client = mqttClient.Client("digi_mqtt_test")  # Create instance of client with client ID “digi_mqtt_test”
client.on_connect = on_connect  # Define callback function for successful connection
client.on_message = on_message  # Define callback function for receipt of a message
# client.connect("m2m.eclipse.org", 1883, 60)  # Connect to (broker, port, keepalive-time)
client.connect(broker, 1883)
client.loop_forever()  # Start networking daemon
