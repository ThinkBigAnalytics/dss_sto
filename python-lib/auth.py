'''
Created on Sep 18, 2017

@author: dt186022

Usage:
 - Saving: write_encrypted(getAuthFilePath(AUTH_FILENAME), db_pwd)
 - Reading: read_encrypted(getAuthFilePath(AUTH_FILENAME))
 - Limitation: assumption is only one db password
'''

from authconstants import *

import dataiku
from dataiku import customrecipe
from binascii import hexlify, unhexlify
from simplecrypt import encrypt, decrypt

import hashlib
import hmac
import base64
import os

def pathExists(filepath):
    return os.path.exists(filepath)

def getCurrentConnectionName(inputDataset):
    #input Dataset is the output of dataiku.Dataset("dataset name")
    return inputDataset.get_location_info().get('info', {}).get('connectionName', '')

def getUserFromConnectionName(name, inputDataset):
    client = dataiku.api_client()
    mydssconnection = client.get_connection(name)
    connectiondef = mydssconnection.get_definition().get(name,{})
    return connectiondef.get('params', {}).get('user', '')
    
def getConnectionUser(inputDataset):
    name = getCurrentConnectionName(inputDataset)
    return getUserFromConnectionName(name, inputDataset)  

def getAuthFilePath(filename):
    try:
        return get_resource_recipe() + filename
    except Exception as e:
        print('Error getting resource recipe: ' + e)
        return AUTH_FILENAME

def getSignature(pwd=MASTER_PASSWORD):
    hostid = os.popen("hostid").read().strip()
    message = bytes(SECRET_KEY + hostid).encode('utf-8')
    secret = bytes(pwd).encode('utf-8')
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
