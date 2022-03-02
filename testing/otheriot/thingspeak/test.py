# #!/usr/bin/python3
# import paho.mqtt.client as mqttClient
# import argparse
# import json
# import time
# import random
# from datetime import datetime
# import sys
# import paho.mqtt.publish as publish
# import string


# channelId = "1663804"
# clientId = "LTUwNBwEMxw3FQQCByQTGyo"
# username = "LTUwNBwEMxw3FQQCByQTGyo"
# password = "s3DJDhwuJC5nbrIpXewu1wmM"
# server = "mqtt3.thingspeak.com"
# port = 1883
# topic = "channels/" + channelId + "/publish"
# Connected = False

# def on_publish(client,userdata,result): #create function for callback
#     print("data published")
#     pass

# def on_connect(client, userdata, flags, rc):
#         print("rc: "+str(rc))
#         sys.stdout.flush()
#         if rc == 0:
#             global Connected 
#             Connected = True
#             print("Connected to broker")
#         else:
#             print("Connection failed")

# client1= mqttClient.Client(client_id=clientId) #create client object
# client1.on_connect= on_connect 
# client1.on_publish = on_publish  #assign function to callback
# client1.username_pw_set(username=username,password=password) #userpass
# client1.connect(server,port) #establish connection
# client1.loop_start()

# while Connected != True:    #Wait for connection
#     time.sleep(0.1)
# msg = {
#     "ts": round(datetime.today().timestamp() * 1000)-700, #today.strftime("%Y-%m-%d %H:%M:%S"),
# 	"id":"11",
# 	"lq":random.randint(0,100),
# 	"x":random.randint(4000,10000) / 100,
# 	"y":random.randint(4000,10000) / 100,
# 	"z":random.randint(4000,10000) / 100
# }
# payload = "field1=" + str(msg['id']) + "&field2=" + str(msg['lq']) + "&field3=" + str(msg['x']) + "&field4=" + str(msg['y']) + "&field5=" + str(msg['z'])
# client1.publish(topic,payload=payload)
 
# # t_transport = "tcp"
# # t_port = 1883
# # publish.single(topic, payload, hostname=server, transport=t_transport, port=t_port, client_id=clientId, auth={'username':username,'password':password})


from datetime import datetime, timezone
import pytz
import time
import ntplib
no = 1

client = ntplib.NTPClient()
server = 'jp.pool.ntp.org'

while True:
    # datetime_now = round(datetime.now(pytz.timezone('Asia/Tokyo')).timestamp() * 1000)
    # datetime_now2 = datetime.now(pytz.timezone('Asia/Tokyo'))
    # # print(no," ",(time.mktime(datetime_now.timetuple())))
    # print(no," ",datetime_now, " - ",(time.mktime(datetime_now2.timetuple())))    
    dt = datetime.now(timezone.utc).timestamp()
    # ntptime = ntplib.system_to_ntp_time(dt)
    # ts = ntplib.ntp_to_system_time(ntptime)
    resp = client.request(server, version=3)
    print(no," ",dt, " - ",resp.orig_time)    
    no=no+1
    time.sleep(5)