import sys
from bson import ObjectId
import json 
from function import *
import datetime
import pandas as pd
from controller import schemaDataController
from controller import sensorController
from pytz import timezone
import statistics
import numpy as np
import matplotlib.pyplot as plt
from scipy.signal import buttap, lp2hp_zpk, bilinear_zpk, zpk2tf, butter

from configparser import ConfigParser

from function import cloud9Lib
config = ConfigParser()
config.read("config.ini")

sensors = []
db = db.dbmongo()
td = datetime.timedelta(hours=int(config["SERVER"]["timediff"]))

collection = "filter"
prefix_collection_sensor = "sensor_data_"
prefix_collection_schema = "schema_data_"

def variancedata(datalist, defaultval=0):
    try:
        df = pd.DataFrame(datalist)
        datalist = df[df[0].apply(lambda x: is_float(x))]
        return statistics.variance(datalist[0])
    except:
        return defaultval

def maxdata(datalist, defaultval=0):
    try:
        df = pd.DataFrame(datalist)
        datalist = df[df[0].apply(lambda x: is_float(x))]
        return max(datalist[0])
    except:
        return defaultval

def mindata(datalist, defaultval=0):
    try:
        df = pd.DataFrame(datalist)
        datalist = df[df[0].apply(lambda x: is_float(x))]
        return min(datalist[0])
    except:
        return defaultval

def is_float(x):
    try:
        float(x)
    except:
        return False
    return True

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

def scipy_high(cutoff_freq, sample_time, x0, x1, x2, y1, y2):
    sample_rate = 1.0 / sample_time
    nyquist_freq = 0.5 * sample_rate
    # nyquist normalized cutoff for digital design
    #1/4 -> 0.25
    #0.5 * 0.25 -> 0.125
    #cutoff can't offer thatn 0.125
    Wn = cutoff_freq / nyquist_freq
    b, a = butter(2, Wn, btype='highpass')
    return -a[1] * y1 - a[2] * y2 + b[0] * x0 + b[1] * x1 + b[2] * x2

def scipy_band(cutoff_freq_low,cutoff_freq_high, sample_time, x0, x1, x2, x3, x4, y1, y2, y3, y4):
    sample_rate = 1.0 / sample_time
    nyquist_freq = 0.5 * sample_rate
    high = cutoff_freq_high / nyquist_freq
    low = cutoff_freq_low / nyquist_freq
    b, a = butter(2, [low,high], btype='bandstop',analog=False)
    return  -a[1] * y1 - a[2] * y2 -a[3] * y3 - a[4] * y4 +  b[0] * x0 + b[1] * x1 + b[2] * x2 + b[3] * x3 + b[4] * x4

def basic_kalman(U,R,H,K,Q,P,U_hat):
    K = P*H / ( H*P*H + R )
    U_hat = U_hat + K*( U-H*U_hat)
    P = (1-K*H)*P + Q
    return U_hat,K,P