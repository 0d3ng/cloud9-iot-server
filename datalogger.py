from matplotlib.font_manager import json_load
import paho.mqtt.client as mqttClient
import json
from datetime import datetime,timedelta
from datetime import timezone as timezone2
from pytz import timezone
import random,string
import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from configparser import ConfigParser
import requests
import threading
import time
import pandas as pd
import statistics

restapi = "http://103.106.72.181:3001/schema/data/"
restapi_sensor = "http://103.106.72.181:3001/device/data/"
sender_email = "iotserver@deltadigitalid.com"
password_email = "iotserver123"

experiment_schema = "n2mjlr"
experiment_data_schema = "8zl2vf"
experiment_sensor = "xs46" #"nt57" #
data_field="ch_1"

current_experiment = {}
current_data = {}
alert_service = {}

client = mqttClient.Client("data_logger_receiver")  # Create instance of client with client ID “digi_mqtt_test”
topic = "sensor/logger"
topic_start="logger/start"
broker="103.106.72.181"#"localhost"#
port=1883
data_field="ch_1"

experiment_state = 0

current_sensor_value=0

def HTTP_post(url,message):
    payload = json.dumps(message)
    headers = {
        'Content-Type': 'application/json'
    }
    response = requests.request("POST", url, headers=headers, data = payload)
    return json.loads(response.text.encode('utf8'))

def send(receiver_email,subject,content):
	message = MIMEMultipart("alternative")
	message["Subject"] = subject
	message["From"] = sender_email
	message["To"] = receiver_email

	part = MIMEText(content, "html")
	message.attach(part)

	context = ssl.create_default_context()
	with smtplib.SMTP_SSL("iix17.cloudhost.id", 465, context=context) as server:
		server.login(sender_email, password_email)
		server.sendmail(
			sender_email, receiver_email, message.as_string()
		)

def add_data(data):
    insertQuery = {}
    now = datetime.now()
    if 'experiment_code' in data: insertQuery['experiment_code'] = data['experiment_code']
    if 'date' in data: 
        insertQuery['date'] = data['date']
    else:
        insertQuery['date'] = now.strftime("%Y-%m-%d")
    if 'start' in data: 
        insertQuery['start'] = data['start'] 
    else: 
        insertQuery['start'] = now.strftime("%H:%M:%S")
    if 'finish' in data: insertQuery['finish'] = data['finish'] 
    if 'temperature_start' in data: insertQuery['temperature_start'] = data['temperature_start'] 
    if 'temperature_end' in data: insertQuery['temperature_end'] = data['temperature_end'] 
    if 'duration' in data: insertQuery['duration'] = data['duration'] 
    if 'temperature_max' in data: insertQuery['temperature_max'] = data['temperature_max'] 
    if 'temperature_min' in data: insertQuery['temperature_min'] = data['temperature_min'] 
    if 'step' in data: insertQuery['step'] = data['step'] 

    add = HTTP_post(restapi+experiment_data_schema+"/add/",insertQuery)
    if(add['status']):
        return 1
    else:
        return 0

def update_data(id,data):
    insertQuery = {}
    now = datetime.now()
    insertQuery['_id'] = id
    if 'experiment_code' in data: insertQuery['name'] = code
    if 'date' in data:insertQuery['date'] = data['date']    
    if 'start' in data: insertQuery['start'] = data['start'] 
    
    if 'finish' in data: 
        insertQuery['finish'] = data['finish'] 
    else:
        insertQuery['finish'] = now.strftime("%H:%M:%S")
    if 'temperature_start' in data: insertQuery['temperature_start'] = data['temperature_start'] 
    if 'temperature_end' in data: insertQuery['temperature_end'] = data['temperature_end'] 
    if 'duration' in data: insertQuery['duration'] = data['duration'] 
    if 'temperature_max' in data: insertQuery['temperature_max'] = data['temperature_max'] 
    if 'temperature_min' in data: insertQuery['temperature_min'] = data['temperature_min']
    if 'step' in data: insertQuery['step'] = data['step'] 
    if 'average' in data: insertQuery['average'] = data['average'] 
    if 'average_rise_per_min' in data: insertQuery['average_rise_per_min'] = data['average_rise_per_min'] 

    add = HTTP_post(restapi+experiment_data_schema+"/edit/",insertQuery)
    if(add['status']):
        return 1
    else:
        return 0

def update_experiment(id,data):
    insertQuery = {}
    now = datetime.now()
    insertQuery['_id'] = id
    if 'start' in data:insertQuery['start'] = data['start']         
    if 'finish' in data:insertQuery['finish'] = data['finish'] 
    if 'state' in data: insertQuery['state'] = data['state'] 
    if 'time_total' in data: insertQuery['time_total'] = data['time_total']      
    add = HTTP_post(restapi+experiment_schema+"/edit/",insertQuery)
    if(add['status']):
        return 1
    else:
        return 0

def get_current_data():
    query = {
        "state": True,    
    }
    get = HTTP_post(restapi+experiment_schema+"/",query)
    if(get['status']):
        current_experiment = get["data"]
        return current_experiment
    else:
        return []

def get_experiment(query):    
    get = HTTP_post(restapi+experiment_schema+"/",query)
    if(get['status']):
        current_experiment = get["data"]
        return current_experiment[0]
    else:
        return []

def get_experiment_data(query):    
    get = HTTP_post(restapi+experiment_data_schema+"/",query)
    if(get['status']):
        experiment_data = get["data"]
        return experiment_data
    else:
        return []

#----------------- Analytical Function

def get_sensor_data(query):    
    get = HTTP_post(restapi_sensor+experiment_sensor+"/",query)
    if(get['status']):
        current_sensor = get["data"]
        return current_sensor
    else:
        return []

def is_float(x):
    try:
        float(x)
    except ValueError:
        return False
    return True

def averagedata(df,defaultval):
    try:
        datalist = df[df.apply(lambda x: is_float(x))]
        return statistics.mean(datalist.to_numpy())
    except:
        print(df)
        print("ERROR Average")
        return defaultval

def variancedata(df,defaultval):
    try:
        datalist = df[df.apply(lambda x: is_float(x))]
        return statistics.variance(datalist.to_numpy())
    except:
        return defaultval

def maxdata(df,defaultval):
    try:
        datalist = df[df.apply(lambda x: is_float(x))]
        return max(datalist.to_numpy())
    except:
        print(datalist)
        print("ERROR Max")
        return defaultval

def mindata(df,defaultval):
    try:
        datalist = df[df.apply(lambda x: is_float(x))]
        return min(datalist.to_numpy())
    except:
        print(datalist)
        print("ERROR Min")
        return defaultval

def analytical_method_1(code):
    query = {
        "experiment_code":code
    }
    experiment_data = get_experiment_data(query)
    for exp_data in experiment_data:
        # print(exp_data)
        _id =  exp_data["id"]
        query = {
            "date_start":exp_data["date"],
            "date_end":exp_data["date"],
            "time_start":exp_data["start"],
            "time_end":exp_data["finish"],
            "sort":{
                "field" : "receive_unix_time",
                "type" : 1
            }
        }
        sensor_data = get_sensor_data(query)
        # print(len(sensor_data))
        df = pd.DataFrame(sensor_data)  
        
        if(exp_data["step"] == 2):        
            average = round(averagedata(df[data_field],0),2)
            min_value = round(mindata(df[data_field],0),2)
            max_value = round(maxdata(df[data_field],0),2)
            variance_value = round(variancedata(df[data_field],0),2)
            # print("Average: "+str(average))
            # print("Min: "+str(min_value)+" "+str(exp_data["temperature_min"]))
            # print("Max: "+str(max_value)+" "+str(exp_data["temperature_max"]))
            # print("Variance: "+str(variance_value))        
            update = update_data(_id,{"average":average})
            
            
        elif (exp_data["step"] == 1):
            df = df[ ["receive_unix_time",data_field] ]
            df["receive_unix_time"] = pd.to_datetime(df['receive_unix_time']/1000,unit='s')
            # print(df)
            # df.to_csv("raw.csv")
            def2 = df.groupby(pd.Grouper(key='receive_unix_time', freq='1min')).first()
            # def2.to_csv("group.csv")
            temp_data = def2[data_field].to_numpy()
            rate_temp = []
            for i in range(1,len(temp_data)-2):
                rate_temp.append( round(temp_data[i+1] - temp_data[i],2) )

            # print(rate_temp)
            try:
                average_rate_temp =  statistics.mean(rate_temp)
            except:
                if(len(temp_data) > 1):
                    average_rate_temp = temp_data[1] - temp_data[0]
                else:
                    average_rate_temp =  0
            # print("Average Temperature Rise Per Min : ",round(average_rate_temp,2))
            # print("Temperature per Min")        
            update = update_data(_id,{"average_rise_per_min":average_rate_temp})

#----------------- Analytical Funtion ---------

def on_connect(client, userdata, flags, rc):
    print("rc: "+str(rc))
    if rc == 0:
        print("Connected to broker:"+broker+":"+str(port))
        Conected=True  
        client.subscribe(topic_start)
        print("Connected to topic:"+topic_start)
        print("-------------------------------")
    else:
        print("Connection failed")

def start_exp_data(now,value,step):
    global current_data      
    current_data['start'] = now.strftime("%H:%M:%S")
    current_data['temperature_start'] = value
    current_data['step'] = step+1
    current_data['temperature_max'] = value

def end_exp_data(experiment_data,now,value):    
    experiment_data['finish'] = now.strftime("%H:%M:%S")
    experiment_data['temperature_end'] = value
    total_time=(datetime.strptime(experiment_data['finish'],'%H:%M:%S') - datetime.strptime(experiment_data['start'],'%H:%M:%S'))
    experiment_data['duration'] = str(total_time).zfill(8)
    insert_status = add_data(experiment_data)
    print("Insert status : ",str(insert_status))    

def worker(experiment,start):
    global experiment_state
    timer = experiment["timer"]
    reminder = experiment["reminder"]
    email_target = experiment["email_target"]
    
    t = datetime.strptime(timer,'%H:%M:%S')
    delta = timedelta(hours=int(timer.split(':')[0]), minutes=int(timer.split(':')[1]), seconds=int(timer.split(':')[2]))
    minutes = delta.total_seconds()/60
    t = int(minutes / (reminder+1))
    reminder_list = []
    finish = start + timedelta(minutes=minutes)    
    for i in range(reminder):
        l = minutes-t
        if (l != 5) and (l != 2) :
            reminder_list.append(l)
        minutes-=t
    reminder_list.append(5)
    reminder_list.append(2)
    reminder_list.sort(reverse=True)
    print(reminder_list)
    
    while True:
        now = datetime.now()
        current_duration=(finish - now)
        current_minutes = current_duration.total_seconds()/60 
        if(len(reminder_list) != 0):                                       
            if current_minutes < reminder_list[0]:            
                global current_sensor_value
                print(str(reminder_list[0])," minutes left")
                print(now.strftime("%Y-%m-%d %H:%M:%S"))
                print(current_sensor_value)
                html = """\
                <html>
                <body>
                    <p>Data Logger Reminder Alerts<br>
                    The current status of Data Logger<br>
                    </p> 
                    <p>
                        Time Left : <b>"""+str(reminder_list[0])+ """ minutes left</b>
                    </p>
                    <p>
                        Temperature : <b>"""+str(current_sensor_value)+ """ &deg; Celcius</b>
                    </p>
                    <p>
                        Current Time  : <b>"""+now.strftime("%Y-%m-%d %H:%M:%S")+ """</b>
                    </p>
                    <br/>
                    <br/>
                    <br/>   
                    <h4><span style='margin: 0;'>Sinceraly, </span></h4>
                    <div><span style='margin: 0;'>SEMAR IoT Server Application Platform</span></div>
                </body>
                </html>
                """
                send(email_target, str(reminder_list[0])+' Minutes Left - Data Logger',html)
                del reminder_list[0] 
        
        if(now>finish and experiment_state == 1):
            print("Break")
            experiment_state = 2
            break

def sensor_message(data, client):
    global experiment_state
    global current_experiment
    global current_data   
    global alert_service 
    global current_sensor_value

    if( data_field in data):
        value = data[data_field]   
        current_sensor_value = value
        if(value >= current_experiment["temperature_target"] and experiment_state == 0):            
            experiment_state = 1
            now = datetime.now()            
            experiment_data = current_data.copy() 
            end_exp_data(experiment_data,now,value)
            current_data = {}           
            current_data["experiment_code"] = current_experiment["experiment_code"]            
            current_data['temperature_min'] = value + 50
            start_exp_data(now,value,experiment_state)
            alert_service = threading.Thread(target=worker, args=(current_experiment,now))
            alert_service.start()

        if(experiment_state == -1):
            experiment_state = 0
            now = datetime.now()              
            current_data['temperature_min'] = value
            start_exp_data(now,value,experiment_state)            
            current_experiment["start"]=now.strftime("%H:%M:%S")                    
    
        if(experiment_state == 2):
            experiment_state = 3
            now = datetime.now()            
            experiment_data = current_data.copy() 
            experiment_data2 = current_data.copy() 
            end_exp_data(experiment_data,now,value)
            current_experiment["finish"]=now.strftime("%H:%M:%S")
            total_time=(datetime.strptime(current_experiment['finish'],'%H:%M:%S') - datetime.strptime(current_experiment['start'],'%H:%M:%S'))
            current_experiment['time_total'] = str(total_time).zfill(8)
            current_experiment['state'] = True
            update_experiment(current_experiment["id"],current_experiment)
            analytical_method_1(experiment_data2["experiment_code"])
            alert_service.join()

        if(value>current_data["temperature_max"]):
            current_data["temperature_max"] = value
        
        if(value<current_data["temperature_min"]):
            if( abs(current_data["temperature_min"] - value) < 50 ):
                current_data["temperature_min"] = value      

def on_message(client, userdata, message):
    global experiment_state
    global current_experiment
    now = datetime.now()
    raw_msg = message.payload.decode("utf-8")
    try:
        data =json.loads(raw_msg)
    except:
        data = []
    if message.topic == topic_start:
        experiment_state = -1
        client.subscribe(topic)
        print("Connected to topic:"+topic)
        code = data["experiment_code"]
        query = {
            "experiment_code": code,    
        }
        current_experiment = get_experiment(query)
        print(current_experiment)
        current_data["experiment_code"] = code
    elif message.topic == topic:
        sensor_message(data,client)
        

# # get_current_data()
# client.on_connect = on_connect  # Define callback function for successful connection
# client.on_message = on_message  # Define callback function for receipt of a message
# # client.connect("m2m.eclipse.org", 1883, 60)  # Connect to (broker, port, keepalive-time)
# client.connect(broker, port)
# client.loop_forever()  # Start networking daemon

    


analytical_method_1("INZKUT")