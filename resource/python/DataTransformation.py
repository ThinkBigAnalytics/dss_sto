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
    # map = json.loads(open('%s/mapping.json' % (os.getenv("DKU_CUSTOM_RESOURCE_FOLDER"))).read())
    # files = [function["file_name"] for function in map if function["category"] == config['category']]
    
    # choices = []
    # for fle in files:
    #     try:
    #         f = json.loads(open('%s/data/%s' % (os.getenv("DKU_CUSTOM_RESOURCE_FOLDER"), fle)).read())
    #         d = {"name":"",
    #              "arguments":"",
    #              "asterarguments":"",
    #              "partitionInputKind":[],
    #              "partitionAttributes":"",
    #              "isOrdered":False,
    #              "orderByColumn":"",
    #              "hasInputTable":False,
    #              "isQueryMode": False,
    #              "queries": []
    #             }
    #         keys = f.keys()
    #         required_input = []
    #         d['partitionInputKind']
    #         unaliased_inputs = {'desc':{}, 'values':[], 'count':0}
    #         if 'function_name' in keys:
    #             d["name"]=f['function_name'].upper()
    #         if 'input_tables' in keys:
    #             d["hasInputTable"] = True
    #             input_tab_lst = f['input_tables']
    #             for input_tab in input_tab_lst:
    #                 required_input_dict = {"isRequired": True, "partitionAttributes":"", "orderByColumn": ""}
    #                 if 'isRequired' in input_tab.keys():
    #                     required_input_dict['isRequired'] = input_tab['isRequired']
    #                 if 'requiredInputKind' in input_tab.keys():
    #                     partitionByKey = input_tab['requiredInputKind'][0]
    #                     if 'partitionByOne' in input_tab.keys() and input_tab['partitionByOne']:
    #                         partitionByKey = "PartitionByOne"
    #                     required_input_dict['kind'] = partitionByKey
    #                 if 'isOrdered' in input_tab.keys():
    #                     required_input_dict['isOrdered'] = input_tab['isOrdered']
    #                 if 'name' in input_tab.keys() or ('Dimension' in input_tab.get('requiredInputKind',[]) and 0 < unaliased_inputs.get('count',0)):
    #                     required_input_dict['name'] = input_tab.get('name', 'Dimension')
    #                     required_input_dict['value'] = ""
    #                     required_input.append(required_input_dict)
    #                 else:
    #                     unaliased_inputs['count'] += 1
    #                     d["isOrdered"] = input_tab['isOrdered'] if 'isOrdered' in input_tab else False
    #                     d['partitionInputKind'] = ['PartitionByOne'] if 'partitionByOne' in input_tab.keys() and input_tab['partitionByOne'] else input_tab['requiredInputKind'] if 'requiredInputKind' in input_tab else []
    #         d["required_input"] = required_input
    #         d["unaliased_inputs"] = unaliased_inputs
    #         if 'argument_clauses' in keys:
    #             a = []
    #             arg_lst = f['argument_clauses']
    #             for argument in arg_lst:
    #                 arg = {"name":"","isRequired":"","value":"", "datatype": "", "allowsLists":True}
    #                 if 'alternateNames' in argument.keys():
    #                     arg["name"]=argument['alternateNames'][0].upper()
    #                 elif 'name' in argument.keys():
    #                     arg["name"]=argument['name'].upper()  
    #                 if 'isRequired' in argument.keys():
    #                     arg["isRequired"]=argument['isRequired']
    #                 if 'datatype' in argument.keys():
    #                     arg["datatype"]=argument['datatype']
    #                 if 'allowsLists' in argument.keys():
    #                     arg["allowsLists"]=argument['allowsLists']
    #                 if 'targetTable' in argument.keys():
    #                     arg["targetTable"] = argument['targetTable']
    #                 if 'isOutputTable' in argument and argument['isOutputTable']:
    #                     arg["isOutputTable"] = argument['isOutputTable']
    #                 a.append(arg)
    #             d["arguments"]=a
    #         if 'cascaded_functions' in keys:
    #             d["cascaded_functions"] = f['cascaded_functions']
    #         choices.append(d);
    #     except ValueError, e:
    #         logging.info("file is not valid json");

    # # Get input table metadata.
    # inputnames = get_input_names_for_role('main')

    # inputtablename = inputs[0]['fullName'].split('.')[0]
    inputDataSets = []
    # print(inputtablename)
    connection = {}
    for input in inputs:
        if(input.get('role') == 'main'):
            inputtablename = input['fullName'].split('.')[1]
            inputDataSets.append(inputtablename)
            if not connection:
                inputdataset = dataiku.Dataset(inputtablename)
                connection = getConnectionParamsFromDataset(inputdataset)
        else:
            inputfoldername = input['fullName'].split('.')[1]       
            inputFolderLocation =  dataiku.Folder(inputfoldername)
    
    # inputSTO = get_input_names_for_role('sto_scripts')
    # inputfoldername = inputS[0]['fullName'].split('.')[1]
    # inputFolderLocation =  dataiku.Folder(inputfoldername)
    # inputFolderDetails = []
    
    folderpath = inputFolderLocation.get_path()
    fileList = []
    listdr = os.listdir(folderpath)
    for item in listdr:
        fileList.append(item)

    return {'inputfolder':folderpath,
            'fileList':fileList,
            'connection': connection,
            'inputDataSets':inputDataSets ,
            'inputs': inputs}
