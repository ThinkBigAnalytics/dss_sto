import dataiku
from dataiku import os
import json
import os
import logging


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
    name = getCurrentConnectionName(inputDataset)
    return getConnectionParams(name)

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
                connection = getConnectionParamsFromDataset(inputdataset)
        else:
            inputfoldername = input['fullName'].split('.')[1]       
            inputFolderLocation =  dataiku.Folder(inputfoldername)

    folderpath = inputFolderLocation.get_path() if inputFolderLocation else ''

    fileList = os.listdir(folderpath) if folderpath else []
    DATA_DIR = "/home/dataiku/dss_data/"
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
