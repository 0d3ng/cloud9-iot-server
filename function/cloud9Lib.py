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
