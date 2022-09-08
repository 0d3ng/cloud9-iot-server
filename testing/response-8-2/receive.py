from matplotlib.font_manager import json_load
import paho.mqtt.client as mqttClient
import json
from datetime import datetime
from datetime import timezone as timezone2
from pytz import timezone
import random,string

broker="103.106.72.188"#"localhost"#
port=1883
topic_l = {}
device_num = 150
max_device = 150
# for i in range(device_num):
#     topic_l.append("/simulationIPS/client-test-"+str(i+1)+"/response")
n = 0
for i in range(device_num):
    if(n not in topic_l):
        topic_l[n] = []
    topic_l[n].append("/simulationIPS/client-test-"+str(i+1)+"/response")
    n+=1
    if n > max_device-1 :
        n=0

device_code = "hl36"
file_goal = str(device_num)+"_"+datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
comm_subs = {}

def randomString(stringLength=8):
    letters1 = string.ascii_lowercase
    letters2 = string.ascii_uppercase 
    letters3 = string.digits
    return ''.join(random.choice(letters1 + letters2 + letters3) for i in range(stringLength))

class Comm:
    def __init__(self, id, code, broker, port, topic):
        self.id = id
        self.code = code
        self.broker = broker
        self.port = int(port)
        self.topic = topic
        
    def on_connect(self,client, userdata, flags, rc):
        print("rc: "+str(rc))
        if rc == 0:
            print("Connected to broker:"+self.broker+":"+str(self.port))
            self.Conected=True  
            for topic in self.topic:
                self.client.subscribe(topic)
                print("Connected to topic:"+topic)
            print("-------------------------------")
        else:
            print("Connection failed")
    
    def on_message(self,client, userdata, message):
        now = datetime.now()
        cDate = now.strftime("%Y-%m-%d")
        cTime = now.strftime("%H:%M:%S")
        cUnix = int(now.timestamp() * 1000)
        raw_msg = message.payload.decode("utf-8")
        data =json.loads(raw_msg)
        id = data["id"]
        writeLog(device_num,id,cDate+";"+cTime+";"+str(cUnix)+";"+raw_msg)       

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

def writeLog(folder,file,value):
    with open(str(folder)+"/receive_"+str(file)+".txt",'a+',newline='') as f:
        f.write('\n'+value)

def on_connect(client, userdata, flags, rc):  # The callback for when the client connects to the broker
    i = 0
    for n in topic_l:
        i=i+1
        code = randomString(3)
        topic = topic_l[n]
        comm_subs[code] = Comm(i,code,broker,port,topic)
        comm_subs[code].connect()


def on_message(client, userdata, message):  # The callback for when a PUBLISH message is received from the server.
    now = datetime.now()
    cDate = now.strftime("%Y-%m-%d")
    cTime = now.strftime("%H:%M:%S")
    cUnix = int(now.timestamp() * 1000)
    raw_msg = message.payload.decode("utf-8")
    writeLog(file_goal,cDate+";"+cTime+";"+str(cUnix)+";"+raw_msg)


client = mqttClient.Client("digi_mqtt_test_receive")  # Create instance of client with client ID “digi_mqtt_test”
client.on_connect = on_connect  # Define callback function for successful connection
client.on_message = on_message  # Define callback function for receipt of a message
# client.connect("m2m.eclipse.org", 1883, 60)  # Connect to (broker, port, keepalive-time)
client.connect(broker, 1883)
client.loop_forever()  # Start networking daemon
