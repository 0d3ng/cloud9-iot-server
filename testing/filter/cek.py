import sys

from pymongo import collection
sys.path.append('../../')
from function import cloud9Lib
import json 
from function import *
from controller import sensorController
import datetime
from datetime import timedelta
import pandas as pd
from pytz import timezone
import statistics

db = db.dbmongo()
td = timedelta(hours=9)
prefix_collection_sensor = "sensor_data_"
prefix_collection_schema = "schema_data_"

def getdata(time_str,time_end,code,search):
    collection = prefix_collection_sensor+str(code)
    datesrc_str = datetime.datetime.strptime(time_str,'%Y-%m-%d %H:%M:%S') - td
    datesrc_end = datetime.datetime.strptime(time_end,'%Y-%m-%d %H:%M:%S') - td
    if(search):
        query = search
    else:
        query = {}
    query['date_add_server'] = {"$gte": datesrc_str,"$lt": datesrc_end}
    exclude = {'raw_message':0}
    limit =  None
    skip = None
    sort = ('date_add_server',1)
    result = sensorController.find(collection,query,exclude,limit,skip,sort)    
    return result

date_start = "2022-04-19"
date_end = "2022-04-19"
time_start = "16:10:00"
time_end = "16:15:59"

data = getdata(date_start+" "+time_start,date_end+" "+time_end,"dn56",{})
# ---------------------------

# ---------------------------
last_time = ""
last_data = []
last_filter_data = [0,0]
filtered_data = []
nonfiltered_data = []
times = []

# for item in data["data"]:
#     ctime = item["date_add_server"]["$date"] / 1000
#     value = item["x"]
#     last_data.append(value)
#     if(len(last_data)>2):
#         print(str(ctime-last_time)+"|"+str(value)+"|"+str(last_data[0])+"|"+str(last_data[1])+"|"+str(last_filter_data[0])+"|"+str(last_filter_data[1]))
#         filter_data = 1
#         last_filter_data.append(filter_data)
#         del last_data[0]
#         del last_filter_data[0]
#     last_time = ctime


# ---------------------------
import statistics

def is_float(x):
    try:
        float(x)
    except :
        return False
    return True

def variancedata(datalist):
    return statistics.variance(datalist)
# ---------------------------
import numpy as np
import matplotlib.pyplot as plt
from scipy.signal import buttap, lp2hp_zpk, bilinear_zpk, zpk2tf, butter
import sympy as sm
def scipy_low(cutoff_freq, sample_time, x0, x1, x2, y1, y2):
    sample_rate = 1.0 / sample_time
    nyquist_freq = 0.5 * sample_rate
    # nyquist normalized cutoff for digital design
    #1/4 -> 0.25
    #0.5 * 0.25 -> 0.125
    #cutoff can't offer thatn 0.125
    Wn = cutoff_freq / nyquist_freq
    b, a = butter(2, Wn, btype='lowpass')
    return -a[1] * y1 - a[2] * y2 + b[0] * x0 + b[1] * x1 + b[2] * x2

cutoff = 0.07

i=0
for item in data["data"]:
    ctime = item["date_add_server"]["$date"] / 1000
    times.append(ctime)
    if is_float(item["x"]) :
        nonfiltered_data.append(item["x"])
    else:
        nonfiltered_data.append(0)
    if i>1:
        filter_data = scipy_low(cutoff, ctime-last_time,
                            nonfiltered_data[i], nonfiltered_data[i-1], nonfiltered_data[i-2],
                            filtered_data[i-1], filtered_data[i-2])
        filtered_data.append(filter_data)
    else:
        filtered_data.append(0)
    last_time = ctime
    i=i+1

# ---------------------------
fig, axes = plt.subplots(2, 1, sharex=True)
plt.rcParams["figure.figsize"] = (25,6)
axes[0].plot(times, nonfiltered_data)
axes[1].plot(times, nonfiltered_data, linewidth=6, alpha=0.75, label='Orginal Low Freqency Signal')
axes[1].plot(times, filtered_data, linewidth=4, label='SciPy Low Pass')
axes[1].legend()
plt.show()

# ---------------------------


# ---------------------------
varr = variancedata(filtered_data)
varr2 = variancedata(nonfiltered_data)
print(str(varr)+" - "+str(varr2))
# ---------------------------