'''
Created on Sep 18, 2017

@author: dt186022

Usage:
 - Saving: write_encrypted(getAuthFilePath(AUTH_FILENAME), db_pwd)
 - Reading: read_encrypted(getAuthFilePath(AUTH_FILENAME))
 - Limitation: assumption is only one db password
'''

from authconstants import *

from dataiku import customrecipe
from binascii import hexlify, unhexlify
from simplecrypt import encrypt, decrypt

import hashlib
import hmac
import base64


def getAuthFilePath(filename):
    try:
        return get_resource_recipe() + filename
    except Exception as e:
        print('Error getting resource recipe: ' + e)
        return AUTH_FILENAME

def getSignature():
    message = bytes(SECRET_KEY).encode('utf-8')
    secret = bytes(MASTER_PASSWORD).encode('utf-8')
    return base64.b64encode(hmac.new(secret, message, digestmod=hashlib.sha256).digest()) 

def write_encrypted(filepath, plaintext):
    try:
        with open(filepath, 'wb') as output:
            tobewritten = encrypt(getSignature(), plaintext.encode('utf8'))
            output.write(hexlify(tobewritten))
    except Exception as e:
        print('Error writing encrypted text: %s',  e)
        
def read_encrypted(filepath):
    try:
        plainsavedtext = ''
        with open(filepath, 'rb') as input:
            ciphersavedtext = input.read()
            plainsavedtext = decrypt(getSignature(), unhexlify(ciphersavedtext))
        return plainsavedtext.decode('utf8')
    except Exception as e:
        print('Error reading encrypted text: %s', e)
        return 'aagdcph'
