import paho.mqtt.client as mqttClient
import json
from datetime import datetime
from datetime import timezone as timezone2
from pytz import timezone
import random,string

broker="localhost"#"103.106.72.188"#
topic = "/simulationIPS/client2/response"
device_code = "hl36"
device_num = 5
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
        try:
            self.client.connect(self.broker,self.port) #connect to broker
            self.client.loop_start()
            print("Connecting to broker:"+self.broker+":"+str(self.port))
        except:
            print(self.broker+": connection failed")
            self.client.loop_stop()

def writeLog(file,value):
    with open("receive_"+file+".txt",'a+',newline='') as f:
        f.write(value+'\n')

def on_connect(client, userdata, flags, rc):  # The callback for when the client connects to the broker
    for val in range(device_num):
        device_id = val+1
        val={
            'channel_code':str(device_id),
            'server':"localhost",#"103.106.72.188",
            'port':1883,
            'topic':"/simulationIPS/client-test-"+str(device_id)+"/response",
            'device_code':"hl36",
            'collection_name':"sensor_benchmark",
            'index_log':"sensor_benchmark"
        }
        comm_subs[device_id] = Comm(val['channel_code'],val['server'],val['port'],val['topic'])
        comm_subs[device_id].connect()

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
