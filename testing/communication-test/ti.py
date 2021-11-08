# #!/usr/bin/python3
# import paho.mqtt.client as paho
# import json,time
# from datetime import datetime
# t = round(datetime.today().timestamp() * 1000)
# print(t)
# # time.sleep(5)
# # s = round(datetime.today().timestamp() * 1000)
# # print(s)
# # print(s - t)
# # print(isinstance(today,int))

# try:
#     today = datetime.utcfromtimestamp(round(t))
# except:
#     today = datetime.utcfromtimestamp(round(t/1000))

# print(today.strftime("%Y-%m-%d %H:%M:%S"))

from datetime import datetime
from pytz import timezone

fmt = "%Y-%m-%d %H:%M:%S %Z%z"

# # Current time in UTC
# now_utc = datetime.now(timezone('Asia/Jakarta')) #datetime.now(timezone('Asia/Jakarta'))
# print(now_utc.strftime(fmt))
# print(datetime.utcnow().strftime(fmt))
# print(round(datetime.now(timezone('Asia/Jakarta')).timestamp() * 1000) )
# print(round(datetime.utcnow().timestamp() * 1000))

# # Convert to US/Pacific time zone
# now_pacific = now_utc.astimezone(timezone('Asia/Singapore'))
# print(now_pacific.strftime(fmt))

# # Convert to Europe/Berlin time zone
# now_berlin = now_pacific.astimezone(timezone('Asia/Jakarta'))
# print(now_berlin.strftime(fmt))
data = {}
data['date_add'] = 1593917125799
today = datetime.fromtimestamp(round(data['date_add']),timezone('Asia/Jakarta'))
today = datetime.fromtimestamp(round(data['date_add']),timezone('Asia/Jakarta')) 
print(today)