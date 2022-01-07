import sys, json, time
from datetime import datetime,timedelta

def worker(code, time_loop):
    next_time = datetime.now() + timedelta(minutes=int(time_loop))
    next_time = next_time.strftime('%Y-%m-%d %H:%M')
    print("next ",next_time)
    while True:
        curentTime = datetime.now()
        curentTime = curentTime.strftime('%Y-%m-%d %H:%M')
        if(next_time == curentTime):
            next_time = datetime.now() + timedelta(minutes=int(time_loop))
            next_time = next_time.strftime('%Y-%m-%d %H:%M')
            print("Process for ",code)
            sys.stdout.flush()

worker("123",1)