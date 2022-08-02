# Using readline()\
import csv
import statistics
import pandas as pd

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


def extracting1(input):
    count = 0
    DS = {}
    file1 = open("send_"+input+".txt", 'r')
    while True:
        line = file1.readline()
        if not line:
            break
        try:
            x = line.split(",")
            times = x[1]
            unix = int(x[2]) 
            msg = x[3] 
            DS[msg] = [times,unix]
        except:
            continue
    file1.close()
    return DS
    
def extracting2(input):
    count = 0
    DS = {}
    file1 = open("receive_"+input+".txt", 'r')
    while True:
        line = file1.readline()
        if not line:
            break
        try:
            x = line.split(",")
            times = x[1]    
            unix = int(x[2]) 
            msg = x[3]    
            DS[msg] = [times,unix]
        except:
            continue
    file1.close()
    return DS

# data = [["node1","iot-node1"],["node2","iot-node2"],["node3","iot-node3"],["node4","iot-node4"],["node5","iot-node5"],["node6","iot-node6"],
        # ["node1","free-node1"],["node2","free-node2"],["node3","free-node3"],["node4","free-node4"],["node5","free-node5"],["node6","free-node6"]]
# data = [["1","1"],["5","5"]]
# data = [["node1","iot-node1"]]
data = ["1","5","10","20","30","40","50","75","100","250","500"]
for i in data:
    device = extracting1(i)
    server = extracting2(i)
    count=0
    count2=0

    error = 0
    error2 = 0

    for d in device:
        count+=1
        if d in server:
            error = error + server[d][1] - device[d][1]
            # print(device[d]," - ",d)
            count2+=1
        else:
            if "::t" not in d:
                error2+=1
    print(error/count2)
    print(count)
    print(count2)
    writeCSV("test",["--------------------------"])
    writeCSV("test",str(i))
    writeCSV("test",[error/count2,count,count2,error2])
    writeCSV("test",["--------------------------"])
    print("------------------")
    