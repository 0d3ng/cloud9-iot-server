import sys
sys.path.append('../../')
import json 
from function import *
from controller import sensorController
from datetime import datetime

f = open('field.json')
field = json.load(f)

for i in field["field"]:
    print(i) 

sys.stdout.flush()