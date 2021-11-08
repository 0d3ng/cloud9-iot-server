import sys
from cryptography.fernet import Fernet
key = b'LqvmTKu5zu_6okVmAa1e2GKOIEoHHuLzaNib9ID6dxs='
print(key)
f = Fernet(key)
plain_text = 'test'
token = f.encrypt(plain_text.encode('utf-8')).decode('utf-8')
print(token)
token = f.decrypt(token.encode('utf-8')).decode('utf-8')
print(token)
sys.stdout.flush()