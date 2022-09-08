# Using readline()\
import csv
import statistics
import pandas as pd
import os.path
from os import path

#log172.28.235.19-->Node1
#log172.28.235.47-->Node2 
#log172.28.235.21-->Node3 
#log172.28.235.26-->Node4 
#log172.28.235.28-->Node5 xxxx 
#log172.28.235.20-->Node6 

periodic = 30

def is_float(x):
    try:
        float(x)
    except ValueError:
        return False
    return True
    
def averagedata(datalist,defaultval):
    try:
        df = pd.DataFrame(datalist)
        datalist = df[df[0].apply(lambda x: is_float(x))]
        return statistics.mean(datalist[0])
    except:
        print(datalist)
        print("ERROR Average")
        return defaultval

def writeCSV(file,value):
    with open(file,'a+', newline='') as f:
        writer=csv.writer(f)
        writer.writerow(value)

def str_to_time(timstr):
    gp = timstr.split(":")
    return (int(gp[0])*3600)+(int(gp[1])*60)+int(gp[2])

def time_to_str(timeint):
    h = int(timeint / 3600)
    m = int( ( timeint - (h*3600) ) / 60 )
    i = timeint - (h * 3600) - (m * 60)
    return str(h)+":"+str(m)+":"+str(i)


def extracting1(folder,input):
    count = 0
    DS = {}
    file1 = open(folder+"/send_"+input+".txt", 'r')
    while True:
        line = file1.readline()
        if not line:
            break
        try:
            x = line.split(";")
            times = x[1]
            unix = int(x[2]) 
            msg = str(x[3]).strip() 
            DS[msg] = unix
        except:
            continue
    file1.close()
    return DS
    
def extracting2(folder,input):
    count = 0
    DS = {}
    file1 = open(folder+"/receive_"+input+".txt", 'r')
    while True:
        line = file1.readline()
        if not line:
            break
        try:
            x = line.split(";")
            times = x[1]    
            unix = int(x[2]) 
            msg = str(x[3]).strip()    
            DS[msg] = unix
        except:
            continue
    file1.close()
    return DS

data = ["1","5","10","20","30","40","50","75","100","125","150"] #"250","500"
data = ["150"] #"250","500"
for i in data:
    if( not path.exists(i) ):
        continue        
    error = 0
    avr = 0
    deltatime = 0
    filldata = 0
    for n in range(1,int(i)+1):
        device = extracting1(i,str(n))
        server = extracting2(i,str(n)) 
        
        for d in device:
            if d in server:
                deltatime += server[d] - device[d]
                filldata+=1
            else:
                error+=1
        # avr +=  (deltatime/filldata)
    # avr = avr / int(i)
    avr = (deltatime/filldata)
    res = ["device",i, "reponse",avr, "loss",error]
    writeCSV("result",res)
        


        
    # device = extracting1(i,n)
    # server = extracting2(i,n)
    # count=0
    # count2=0

    # error = 0
    # error2 = 0

    # for d in device:
    #     count+=1
    #     if d in server:
    #         error = error + server[d][1] - device[d][1]
    #         # print(device[d]," - ",d)
    #         count2+=1
    #     else:
    #         if "::t" not in d:
    #             error2+=1
    # print(error/count2)
    # print(count)
    # print(count2)
    # writeCSV("test",["--------------------------"])
    # writeCSV("test",str(i))
    # writeCSV("test",[error/count2,count,count2,error2])
    # writeCSV("test",["--------------------------"])
    # print("------------------")
    