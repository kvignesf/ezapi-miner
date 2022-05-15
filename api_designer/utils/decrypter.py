import hashlib

from Cryptodome.Cipher import AES
from hashlib import md5
import base64


password = 'ezapidbpwdhandshake'

BLOCK_SIZE = 16

def pad (data):
    pad = BLOCK_SIZE - len(data) % BLOCK_SIZE
    return data + pad * chr(pad)

def unpad (padded):
    #pad = ord(padded[-1])
    pad = padded[-1]
    return padded[:-pad]

def _encrypt(data, nonce, password):
    m = hashlib.md5()
    #password = password.encode("utf-8")
    m.update(password.encode('utf-8'))
    key = m.hexdigest()

    m = hashlib.md5()
    m.update(bytes(password, 'utf-8') + bytes(key, "utf-8"))
    iv = m.hexdigest()

    data = pad(data)

    aes = AES.new(key.encode('utf-8'), AES.MODE_CBC, iv[:16].encode('utf-8'))

    encrypted = aes.encrypt(data.encode('utf-8'))
    return base64.urlsafe_b64encode(encrypted)

def _decrypt(edata, nonce, password):
    try:
        edata = base64.urlsafe_b64decode(edata)

        m = md5()
        m.update(password.encode('utf-8'))
        key = m.hexdigest()

        m = md5()
        m.update(bytes(password, 'utf-8') + bytes(key, 'utf-8'))
        iv = m.hexdigest()

        aes = AES.new(key.encode('utf-8'), AES.MODE_CBC, iv[:16].encode('utf-8'))
        origString = unpad(aes.decrypt(edata))
    except:
        origString = 'invalidencryption'
    return origString

# string = "Hello World"
# arr = bytes(string, 'utf-8')
# arr2 = bytes(string, 'ascii')
# print("arr", arr)
# print("arr2", arr2)
# output = _encrypt(input, "", password)
# print(output)
# plaintext = _decrypt(output, "", password)
# print(bytes.decode(plaintext))
# output = 'UhbVSFjVt+gidWEGsnmqVA=='
# print(bytes(output, 'utf-8'))
# plaintext2 = _decrypt(bytes(output, 'utf-8'), "", password)
# print(bytes.decode(plaintext2))

