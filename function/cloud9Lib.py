import json, sys
from types import SimpleNamespace as Namespace
import random,string

from cryptography.fernet import Fernet
from Cryptodome.Cipher import AES

import re 

key = b'LqvmTKu5zu_6okVmAa1e2GKOIEoHHuLzaNib9ID6dxs='

def jsonObject(data):
	data = json.dumps(data,default=str)	
	return json.loads(data)

def randomString(stringLength=8):
    letters1 = string.ascii_lowercase
    letters2 = string.ascii_uppercase 
    letters3 = string.digits
    return ''.join(random.choice(letters1 + letters2 + letters3) for i in range(stringLength))

def randomStringLower(stringLength=8):
    letters1 = string.ascii_lowercase
    letters2 = string.digits
    return ''.join(random.choice(letters2 + letters1 + letters2 ) for i in range(stringLength))

def randomOnlyString(stringLength=8):
    letters1 = string.ascii_lowercase
    return ''.join(random.choice(letters1) for i in range(stringLength))

def randomNumber(stringLength=8):
    letters3 = string.digits
    return ''.join(random.choice(letters3) for i in range(stringLength))

def encrypt(plain_text):  
    f = Fernet(key)
    token = f.encrypt(plain_text.encode('utf-8'))
    token = token.decode('utf-8')
    return token

def decrypt(plain_text):  
    f = Fernet(key)
    token = f.decrypt(plain_text.encode('utf-8'))
    token = token.decode('utf-8')
    return token

def validEmail(email):  
    regex = '^\w+([\.-]?\w+)*@\w+([\.-]?\w+)*(\.\w{2,3})+$'
    if(re.search(regex,email)):  
        return True            
    else:  
        return False

def delimeterExtract(msg):
    filter = msg.split(":")
    print("-------Filter-------")
    print(filter)
    print("--------------------")
    sys.stdout.flush()
    result = {}
    for val in filter:
        item = val.split("=")
        if(len(item) == 2):  
            key = item[0]
            if is_int(item[1]):
                value = int(item[1])
            elif is_float(item[1]):
                value = float(item[1])
            else:
                value = item[1]
            result[key] = value
    print("-------Result-------")
    print(result)
    print("--------------------")
    return jsonObject(result)

def is_int(n):
    try:
        float_n = float(n)
        int_n = int(float_n)
    except ValueError:
        return False
    else:
        return float_n == int_n

def is_float(n):
    try:
        float_n = float(n)
    except ValueError:
        return False
    else:
        return True

def str2bool(v):
  return v.lower() in ("yes", "true", "t", "1")