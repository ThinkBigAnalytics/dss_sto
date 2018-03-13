# -*- coding: utf-8 -*-
'''
Copyright © 2018 by Teradata.
Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
associated documentation files (the "Software"), to deal in the Software without restriction,
including without limitation the rights to use, copy, modify, merge, publish, distribute,
sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all copies or
substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT
NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
'''

'''
Created on Sep 18, 2017

@author: dt186022
'''

import dataiku
from dataiku.customrecipe import *

import os

def pathExists(filepath):
    return os.path.exists(filepath)

def getCurrentConnectionName(inputDataset):
    #input Dataset is the output of dataiku.Dataset("dataset name"
    return inputDataset.get_location_info().get('info', {}).get('connectionName',
                                                                '')

def getConnectionParams(name):
    client = dataiku.api_client()
    mydssconnection = client.get_connection(name)
    return mydssconnection.get_definition().get('params', {})

def getConnectionParamsFromDataset(inputDataset):
    name = getCurrentConnectionName(inputDataset)
    return getConnectionParams(name)

def getConnectionUser(inputDataset):
    return getConnectionParamsFromDataset(inputDataset).get('user', '')

def getAuthFilePath(filename):
    filepath = AUTH_FILENAME
    try:
        filepath = ('{directory}/conn_info/{filename}'.format(directory=get_recipe_resource(),
                                                filename=filename))
    except Exception as e:
        print('Error getting resource recipe: {error}'.format(error=repr(e.args)))
    return filepath
