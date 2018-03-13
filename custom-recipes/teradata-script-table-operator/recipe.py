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

# Code for custom code recipe KC_TestPlugins_compute_pythonrecipe_out (imported from a Python recipe)

# To finish creating your custom recipe from your original PySpark recipe, you need to:
#  - Declare the input and output roles in recipe.json
#  - Replace the dataset names by roles access in your code
#  - Declare, if any, the params of your custom recipe in recipe.json
#  - Replace the hardcoded params values by acccess to the configuration map

# See sample code below for how to do that.
# The code of your original recipe is included afterwards for convenience.
# Please also see the "recipe.json" file for more information.

# import the classes for accessing DSS objects from the recipe
import dataiku
# Import the helpers for custom recipes
from dataiku.customrecipe import *

#CALL Subprocess for BTEQ script
from subprocess import call

from auth import *
from pynbExtractor import *
from re import search
from shutil import copyfile


# Inputs and outputs are defined by roles. In the recipe's I/O tab, the user can associate one
# or more dataset to each input and output role.
# Roles need to be defined in recipe.json, in the inputRoles and outputRoles fields.

# To  retrieve the datasets of an input role named 'input_A' as an array of dataset names:
input_A_names = get_input_names_for_role('main')
# The dataset objects themselves can then be created like this:
input_A_datasets = [dataiku.Dataset(name) for name in input_A_names]

# To  retrieve the datasets of an input role named 'input_A' as an array of dataset names:
input_B_names = get_input_names_for_role('sto_scripts')

# The dataset objects themselves can then be created like this:
input_B_datasets = [dataiku.Dataset(name) for name in input_B_names]

# For outputs, the process is the same:
output_A_names = get_output_names_for_role('main')
output_A_datasets = [dataiku.Dataset(name) for name in output_A_names]




# The configuration consists of the parameters set up by the user in the recipe Settings tab.

# Parameters must be added to the recipe.json file so that DSS can prompt the user for values in
# the Settings tab of the recipe. The field "params" holds a list of all the params for wich the
# user will be prompted for values.

# The configuration is simply a map of parameters, and retrieving the value of one of them is simply:
# my_variable = get_recipe_config()['parameter_name']

# For optional parameters, you should provide a default value in case the parameter is not present:
# my_variable = get_recipe_config().get('parameter_name', None)

# config variable
function_config = get_recipe_config().get('function', None)

# Note about typing:
# The configuration of the recipe is passed through a JSON object
# As such, INT parameters of the recipe are received in the get_recipe_config() dict as a Python float.
# If you absolutely require a Python int, use int(get_recipe_config()["my_int_param"])


#############################
# Your original recipe
#############################

# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
import os
from dataiku import pandasutils as pdu
from dataiku.core.sql import SQLExecutor2

#DataIku Managed Folder Handler (Specifically sto_scripts as of now)
handle = dataiku.Folder("sto_scripts") if input_B_names else None


# Recipe inputs
# print('Getting DB User')
# def db_user():
#     return getConnectionUser(output_A_datasets[0])
print('Getting Default Database')
def default_database(dataset=output_A_datasets[0]):
    return getConnectionParamsFromDataset(output_A_datasets[0]).get('defaultDatabase', "");

def getConnectionDetails(dataset=input_A_datasets[0]):
    return getConnectionParamsFromDataset(input_A_datasets[0]);


properties = getConnectionDetails(input_A_datasets[0]).get('properties')
autocommit = getConnectionDetails(input_A_datasets[0]).get('autocommitMode')
tmode = ''

for prop in properties:
    if prop['name'] == 'TMODE' and prop['value'] == 'TERA':
        #Detected TERA
        print('I am in TERA MODE')
        tmode = 'TERA'
        stTxn = 'BEGIN TRANSACTION;'
        edTxn = 'END TRANSACTION;'

    elif prop['name'] == 'TMODE' and prop['value'] == 'ANSI':
        #Detected ANSI
        print('I am in ANSI MODE')
        tmode = 'ANSI'
        stTxn = ';'
        edTxn = 'COMMIT WORK;'

empty_table = input_A_datasets[0]
#SQL Executor
executor = SQLExecutor2(dataset=empty_table)

defaultDB = default_database()
if default_database(input_A_datasets[0]) != default_database():
    raise RuntimeError('Input dataset and output dataset have different connection details')

output_location = output_A_datasets[0].get_location_info()['info']

print('Getting Function Config')
scriptAlias = function_config.get('script_alias', '')
scriptFileName = function_config.get('script_filename', '')
scriptLocation = function_config.get('script_location', '')
searchPath = default_database()
outputTable = output_A_datasets[0].get_location_info()['info'].get('table', '')
partitionOrHash = function_config.get('partitionOrHash', '')

performFileLoad = True

#Script Location for main script
if(scriptLocation == 'sz'):
    scriptFileLocation = function_config.get('script_filelocation')
elif 'czp' == scriptLocation:
    scriptFileLocation = pynbDestinationPath(scriptFileName)
    writePythonNotebookToResourceFolder(output_A_names[0].split('.')[0], scriptFileName)
    scriptFileName = scriptFileName.replace("'", "")
    scriptFileName = scriptFileName.replace(" ", "")
elif 'cz' == scriptLocation:
    scriptFileLocation = handle.file_path(scriptFileName)
else:
    performFileLoad = False
    


scriptLocation = scriptLocation[:2]

commandType = function_config.get('command_type', '')
returnClause = ', '.join((x.get('name', '') + ' ' + x.get('type', ''))
                         for x in function_config.get('return_clause', []))
scriptArguments = ', '.join(x.get('value', '')
                            for x in function_config.get('arguments', []))
additionalFiles = function_config.get('files')

def getHashClause(hasharg):
    return hasharg and ('\n             HASH BY {hasharg}'.format(hasharg=hasharg))
def getPartitionClause(partitionarg):
    return partitionarg and ('\n             PARTITION BY {partitionarg}'\
                             .format(partitionarg=partitionarg))
def getOrderClause(orderarg):
    return orderarg and ('\n             ORDER BY {orderarg}'.format(orderarg=orderarg))

def getLocalOrderClause(localorderarg):
    return localorderarg and ('\n             LOCAL ORDER BY {localorderarg}'.format(localorderarg=localorderarg))

def getAdditionalClauses(arg):
    return arg and ('\n{arg}'.format(arg=arg))

def getSelectInstalledFileQuery(databasename, fileAlias):
    return """select * from dbc.tables
where databasename = '{dataset}'
and TableName = '{table}'
and TableKind = 'Z';""".format(dataset=databasename, table=fileAlias)


if(partitionOrHash == 'part'):
    print('Partition')
    partitionClause = getPartitionClause(function_config.get('partitionby', ''))
    orderClause = getOrderClause(function_config.get('orderby', ''))
    hashClause = ''
    localOrderByClause = ''
elif(partitionOrHash == 'hash'):
    print('Hash')
    hashClause = getHashClause(function_config.get('hashby', ''))
    localOrderByClause = getLocalOrderClause(function_config.get('localorderby', ''))
    partitionClause = ''
    orderClause = ''
else: 
    print('No Partition/Hash')
    hashClause = ''
    localOrderByClause = ''
    partitionClause = ''
    orderClause = ''
    


print('Building STO Script Command')
script_command = ''
if commandType == 'python':
    script_command = """'export PATH; python ./"""+searchPath+"""/"""+scriptFileName+""" """+scriptArguments+"""'"""
elif commandType == 'r':
    script_command = """'R --vanilla --slave -f ./"""+searchPath+"""/"""+scriptFileName+"""'"""
elif commandType == 'other':
    script_command = """'"""+function_config.get('other_command', '')+"""'"""
print("""Script Command: """+script_command)

# select query
print('Building Database Query')
databaseQuery = 'DATABASE {database};'.format(database=default_database())
print("""Database Query: """+databaseQuery)
print('Build Session SearchUIFDBPath Query')
setSessionQuery = 'SET SESSION SEARCHUIFDBPATH = {searchPath};'.format(searchPath=searchPath)
print("""Set Session Query: """ + setSessionQuery)
etQuery = 'COMMIT WORK;'

#File Related:

removeFileQuery = """CALL SYSUIF.REMOVE_FILE('""" + scriptAlias + """',1);"""
print('Building Script installation query')
installFileQuery = """CALL SYSUIF.INSTALL_FILE('""" + escape(scriptAlias) + """','""" + escape(scriptFileName) + """','"""+escape(scriptLocation)+"""!"""+escape(scriptFileName)+"""');"""
replaceFileQuery = """CALL SYSUIF.REPLACE_FILE('""" + escape(scriptAlias) + """','""" + escape(scriptFileName) + """','"""+escape(scriptLocation)+"""!"""+escape(scriptFileName)+"""', 0);"""
scriptDoesExist = """select * from dbc.tables
where databasename = '{searchPath}'
and TableKind = 'Z';""".format(searchPath=searchPath)

#File Copy to DIST
dkuinstalldir = os.environ['DKUINSTALLDIR']
newPath = dkuinstalldir + """/dist/"""+scriptFileName
print(newPath)
# print(replaceFileQuery)
#COPY FILE TEST
if(performFileLoad):
    copyfile(escape(scriptFileLocation.rstrip()), newPath)

#ADDITIONAL FILES
#INSTALL Additional files
print('Building Additional File INSTALLATION/REPLACEMENT')
# installAdditionalFiles = """"""
installAdditionalFilesArray = []
for item in additionalFiles:
    address = item.get('file_address', '').rstrip() if\
        ('s' == item.get('file_location', '')) else handle.file_path(item.get('filename', ''))
    newPath = dkuinstalldir + """/dist/"""+item.get('filename')
    print(newPath)
    # print(replaceFileQuery)
    print(address)
    #COPY FILE TEST
    copyfile(address, newPath)
    if item.get('replace_file'):
        # installAdditionalFiles = installAdditionalFiles + """\nCALL SYSUIF.REPLACE_FILE('""" + item.get('file_alias') + """','""" + item.get('filename') + """','"""+item.get('file_location')+item.get('file_format')+"""!"""+address+"""',0);"""
        tableCheck = executor.query_to_df(getSelectInstalledFileQuery(defaultDB,scriptAlias))
        print('Checking table list for previously installed files')
        print(tableCheck)
        print(tableCheck.shape)
        if(tableCheck.shape[0] < 1):
            print("""File Alias:"""+ item.get('file_alias'))
            print('Was not able to find the file in the table list. Attempting to use INSTALL_FILE')        
            installAdditionalFilesArray.append("""\nCALL SYSUIF.INSTALL_FILE('""" + item.get('file_alias') + """','""" + item.get('filename') + """','"""+item.get('file_location')+item.get('file_format')+"""!"""+item.get('filename')+"""');""")
        else:    
            print("""File Alias:"""+ item.get('file_alias'))
            print('Was able to find the file in the table list. Attempting to use REPLACE_FILE')                
            installAdditionalFilesArray.append("""\nCALL SYSUIF.REPLACE_FILE('""" + item.get('file_alias') + """','""" + item.get('filename') + """','"""+item.get('file_location')+item.get('file_format')+"""!"""+item.get('filename')+"""',0);""")
    else:
        installAdditionalFilesArray.append("""\nCALL SYSUIF.INSTALL_FILE('""" + item.get('file_alias') + """','""" + item.get('filename') + """','"""+item.get('file_location')+item.get('file_format')+"""!"""+item.get('filename')+"""');""")
print("""Additional Files Installation Query/ies: """)
print(installAdditionalFilesArray)

#MOVE ADDITIONAL FILES


print(output_A_names[0])
useSQLOnClause = function_config.get('useSQLOnClause')

onClause = function_config.get('sql_on_clause')
selectClause = function_config.get('select_clause')

STOQuery = """SELECT {selectClause}
FROM SCRIPT (ON ({onClause}){hashClause}{localOrderClause}{partitionClause}{orderClause}
             SCRIPT_COMMAND({script_command})
             RETURNS ('{returnClause}')
            ) {additionalClauses};""".\
            format(tabletype=function_config.get('table_type', ''),
                   searchPath=searchPath,
                   outputTable=outputTable,
                   selectClause=selectClause,
                   onClause=onClause,
                   script_command=script_command,
                   hashClause=hashClause,
                   partitionClause=partitionClause,
                   orderClause=orderClause,
                   localOrderClause=localOrderByClause,
                   returnClause=returnClause,
                   additionalClauses=getAdditionalClauses(function_config.get('add_clauses','')))

def getSelectTableQuery(databasename, fileAlias):
    return """select * from dbc.tables
where databasename = '{dataset}'
and TableName = '{table}'
and TableKind = 'T';""".format(dataset=databasename, table=fileAlias)

def database():
    # for now, database name = db user name
    return default_database()
    

#File Loading
if(performFileLoad):
    if function_config.get("replace_script"):
        print('performing replacefile')
        tableCheck = executor.query_to_df(getSelectInstalledFileQuery(defaultDB,scriptAlias))
        print('Checking table list for previously installed files')
        print(tableCheck)
        print(tableCheck.shape)
        if(tableCheck.shape[0] < 1):
            print('Was not able to find the file in the table list. Attempting to use INSTALL_FILE')
            if autocommit:
                print('Auto commit is true')
                executor.query_to_df(installFileQuery,[setSessionQuery])
            else:
                print('Auto commit is false')
                executor.query_to_df(edTxn,[stTxn, setSessionQuery, installFileQuery])
        else:    
            print('Was able to find the file in the table list. Attempting to use REPLACE_FILE')
            if autocommit:
                print('Auto commit is true')
                executor.query_to_df(replaceFileQuery,[setSessionQuery])
            else:
                print('Auto commit is false')
                executor.query_to_df(edTxn,[stTxn, setSessionQuery, replaceFileQuery])
    else:
        print('performing installfile')
        executor.query_to_df(edTxn,[stTxn, setSessionQuery, installFileQuery])

if(installAdditionalFilesArray != []):
    print('Installing additional files...')
    executor.query_to_df(edTxn,[stTxn,setSessionQuery]+installAdditionalFilesArray)
    
# Recipe outputs                                                          
print('setSessionQuery')
print(setSessionQuery)
print('replaceFileQuery')
print(replaceFileQuery)
print('Executing SELECT Query...')
print(STOQuery)
selectResult = executor.query_to_df(STOQuery,[setSessionQuery])
print('Moving results to output...')
pythonrecipe_out = output_A_datasets[0]
pythonrecipe_out.write_with_schema(selectResult)
print('Complete!')  
