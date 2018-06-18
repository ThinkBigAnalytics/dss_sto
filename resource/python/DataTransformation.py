import dataiku
from dataiku import os
import json
import os
import logging

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

FUNCTION_CATEGORY="Data Transformation"

def getCurrentConnectionName(inputDataset):
    #input Dataset is the output of dataiku.Dataset("dataset name"
    return inputDataset.get_location_info().get('info', {}).get('connectionName',
                                                                '')

def getConnectionParams(name):
    client = dataiku.api_client()
    mydssconnection = client.get_connection(name)
    return mydssconnection.get_definition().get('params', {})

def getConnectionParamsFromDataset(inputDataset):
    return inputDataset.get_location_info(sensitive_info=True)['info']

# paylaod is sent from the javascript's callPythonDo()
# config and plugin_config are the recipe/dataset and plugin configured values
# inputs is the list of input roles (in case of a recipe)
def do(payload, config, plugin_config, inputs):
    inputDataSets = []
    # print(inputtablename)
    connection = {}
    project = ''
    inputFolderLocation = None
    for input in inputs:
        if(input.get('role') == 'main'):
            inputtablename = input['fullName'].split('.')[1]
            project = input['fullName'].split('.')[0]
            inputDataSets.append(inputtablename)
            if not connection:
                inputdataset = dataiku.Dataset(inputtablename)
                connection = getConnectionParamsFromDataset(inputdataset).get('connectionParams', {})
        else:
            inputfoldername = input['fullName'].split('.')[1]       
            inputFolderLocation =  dataiku.Folder(inputfoldername)

    folderpath = inputFolderLocation.get_path() if inputFolderLocation else ''

    fileList = os.listdir(folderpath) if folderpath else []
    DATA_DIR = os.environ["DIP_HOME"]
    PYNBDIR = "config/ipython_notebooks/"
    pypath = os.path.join(DATA_DIR, PYNBDIR, project)
    pynbList = filter(lambda f: not f.startswith('.'), os.listdir(pypath)) if\
        os.path.exists(pypath) else []

    return {'inputfolder':folderpath,
            'fileList':fileList,
            'nbList':pynbList,
            'connection': connection,
            'inputDataSets':inputDataSets ,
            'inputs': inputs}
