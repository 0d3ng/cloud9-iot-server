from datetime import datetime,timedelta
from configparser import ConfigParser
from importlib import reload
from pytz import timezone
import random
import time

time_loop = 5
last_time = datetime.now(timezone('Asia/Tokyo')).strftime('%Y-%m-%d %H:%M:%S')
next_time = datetime.strptime(last_time,'%Y-%m-%d %H:%M:%S') + timedelta(seconds=int(time_loop))
# curentTime = datetime.now(timezone('Asia/Tokyo'))
# print(datetime.timestamp(next_time))
# print(datetime.timestamp(curentTime))

while True:
    curentTime = datetime.now(timezone('Asia/Tokyo'))
    if(datetime.timestamp(next_time) <= datetime.timestamp(curentTime)):
        curentTime = next_time
        next_time = next_time + timedelta(seconds=int(time_loop))
        print(next_time)
        last_time = curentTime
        t = random.randint(3, 9)
        print(t)
        time.sleep(t)




# print(next_time)
# cek =  (datetime.timestamp(datetime.strptime(last_time,'%Y-%m-%d %H:%M:%S')) + 30)*1000
# cek2 = datetime.timestamp(next_time)*1000
# print(str(cek) +" = "+str(cek2))
# # while True :
# curentTime = datetime.now(timezone('Asia/Tokyo'))
# unix_timestamp = datetime.timestamp(curentTime)*1000
# print(curentTime)
# print(unix_timestamp)