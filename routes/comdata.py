import sys
sys.path.append('../')
from tornado.web import RequestHandler
from bson import ObjectId
import json 
from function import *
from controller import comChannelController
from controller import commETLController
from controller import commLogController
from datetime import datetime 
from datetime import timezone as timezone2
from pytz import timezone

groups = []


#PRIMARY VARIABLE - DONT DELETE
define_url = [
    ['sensor/([^/]+)/','add']
]

def writeLog(file,value):
    with open("log/log_"+file+".log",'a+',newline='') as f:
        f.write(value+'\n')

class add(RequestHandler):
  def post(self,token_access):        
    try:
        data = json.loads(self.request.body)
    except:
        raw_msg = str(self.request.body).replace('\u0000', '')
        data = cloud9Lib.delimeterExtract(raw_msg)    

    receive_unix_time = round(datetime.now(timezone('Asia/Tokyo')).timestamp()*1000)
    cDate = datetime.now(timezone('Asia/Tokyo')).strftime("%Y-%m-%d")
    cTime = datetime.now(timezone('Asia/Tokyo')).strftime("%H:%M:%S")
    writeLog("HTTP_"+token_access+"_"+cDate,cDate+","+cTime+","+str(receive_unix_time)+","+str(self.request.body))

    if not token_access:
        response = {"status":False, "message":"Token Acces not found",'data':data}               
        self.write(response)
        return
    #Log Insert#
    remote_ip = self.request.headers.get("X-Real-IP") or \
            self.request.headers.get("X-Forwarded-For") or \
            self.request.remote_ip
    insertLog = {
        'token_access' : token_access,
        'ip_sender':remote_ip,
        'channel_type':'http-post',
        'raw_message':data
    }
    #End Log Insert#
    queryGroup = {
        'token_access' : token_access,
        'channel_type':"http-post",
        'active': True 
    }
    resultChannel = comChannelController.findOne(queryGroup)
    if not resultChannel['status']:
        response = {"status":False, "message":"Token Access not match",'data':json.loads(self.request.body)} 
        insertLog['response'] = response
        commLogController.add(insertLog)
        self.write(response)
        return              
    else:
        channelData = resultChannel['data']
        ###WriteLog                
    
    infoHttp = {
        'token_access' : token_access,
        'ip_sender':remote_ip,
        'channel_type':'http-post',
    }
    
    
    
    if 'date_add' in data :
        try:
            if(isinstance(data['date_add'],int)):
                infoHttp['date_add_sensor_unix'] = data['date_add']
                try:
                    today = datetime.fromtimestamp(round(data['date_add']),timezone('Asia/Tokyo')) #datetime.fromtimestamp(round(message['date_add']))
                except:
                    today = datetime.fromtimestamp(round(data['date_add']/1000),timezone('Asia/Tokyo')) #datetime.fromtimestamp(round(message['date_add']/1000))
                infoHttp['date_add_sensor'] = today
            else:
                infoHttp['date_add_sensor'] = datetime.strptime(data['date_add'],'%Y-%m-%d %H:%M:%S')
        except:
            print("error")
            infoHttp['date_add_sensor'] = data['date_add']
    else :
        infoHttp['date_add_sensor'] = None
    
    
    if 'device_code' in data :
        insert = commETLController.etl(channelData['collection_name'],channelData['index_log'],infoHttp,data['device_code'],data)
    elif 'device_code' in channelData :
        insert = commETLController.etl(channelData['collection_name'],channelData['index_log'],infoHttp,channelData['device_code'],data)
    else :
        insert = commETLController.nonetl(channelData['collection_name'],channelData['index_log'],infoHttp,data)
    
    if not insert['status']:
        response = {"status":False, "message":"Failed to add", 'data':json.loads(self.request.body)}               
    else:
        response = {'message':'Success','status':True}    
    insertLog['response'] = response
    commLogController.add(insertLog)
    self.write(response)