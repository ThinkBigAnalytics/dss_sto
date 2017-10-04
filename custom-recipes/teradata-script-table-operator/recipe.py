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
from re import search

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

print('Checks')
print(function_config)
print(output_A_names)
print(output_A_datasets)

# print('Starting handle')
handle = dataiku.Folder("sto_scripts")
# filepath = handle.file_path("ex2p.py")
# filepath ="/home/dataiku/dss_data/managed_folders/DT186022_TEST/kA2too62/"

# path = handle.get_path()

# print('Check folder directory')
# print(handle)
# print(path)
# print("Stuff")
# print(os.listdir("/home/dataiku/dss_data/managed_folders/DT186022_TEST/kA2too62/"))


# print(jsonfile)
# Recipe inputs
# empty_table = dataiku.Dataset("empty_table")
def db_user():
    return getConnectionUser(output_A_datasets[0])
empty_table = input_A_datasets[0]
output_location = output_A_datasets[0].get_location_info()['info']

scriptAlias = function_config.get('script_alias')
scriptFileName = function_config.get('script_filename')
scriptLocation = function_config.get('script_location')
searchPath = db_user()
outputTable = output_A_datasets[0].get_location_info()['info'].get('table', '')


if(scriptLocation == 'sz'):
    scriptFileLocation = function_config.get('script_filelocation')
else:
    scriptFileLocation = handle.file_path(scriptFileName)

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

def getAdditionalClauses(arg):
    return arg and ('\n{arg}'.format(arg=arg))

partitionClause = getPartitionClause(function_config.get('partitionby', ''))
hashClause = getHashClause(function_config.get('hashby', ''))
orderClause = getOrderClause(function_config.get('orderby', ''))

script_command = ''
if commandType == 'python':
    script_command = """'export PATH; python ./"""+searchPath+"""/"""+scriptFileName+""" """+scriptArguments+"""'"""
elif commandType == 'r':
    script_command = """'R --vanilla --slave -f ./"""+searchPath+"""/"""+scriptFileName+"""'"""

#INSTALL Additional files
installAdditionalFiles = """"""
for item in additionalFiles:
    if item.get('replace_file'):
        installAdditionalFiles = installAdditionalFiles + """\nCALL SYSUIF.REPLACE_FILE('""" + item.get('file_alias') + """','""" + item.get('filename') + """','"""+item.get('file_location')+item.get('file_format')+"""!"""+item.get('file_address').rstrip()+"""',0);"""        
    else:
        installAdditionalFiles = installAdditionalFiles + """\nCALL SYSUIF.INSTALL_FILE('""" + item.get('file_alias') + """','""" + item.get('filename') + """','"""+item.get('file_location')+item.get('file_format')+"""!"""+item.get('file_address').rstrip()+"""');"""

# select query
setSessionQuery = 'SET SESSION SEARCHUIFDBPATH = {searchPath};'.format(searchPath=searchPath)
etQuery = 'COMMIT WORK;'
removeFileQuery = """CALL SYSUIF.REMOVE_FILE('""" + scriptAlias + """',1);"""
# installFileQuery = """CALL SYSUIF.INSTALL_FILE('""" + function_config.get('script_alias') + """','""" + function_config.get('script_filename') + """','cz!"""+filepath+"""');"""
installFileQuery = """CALL SYSUIF.INSTALL_FILE('""" + scriptAlias + """','""" + scriptFileName + """','"""+scriptLocation+"""!"""+scriptFileLocation.rstrip()+"""');"""

#sz if in DB
replaceFileQuery = """CALL SYSUIF.REPLACE_FILE('""" + scriptAlias + """','""" + scriptFileName + """','"""+scriptLocation+"""!"""+scriptFileLocation.rstrip()+"""', 0);"""
scriptDoesExist = """select * from dbc.tables
where databasename = '{searchPath}'
and TableKind = 'Z';""".format(searchPath=searchPath)

print(output_A_names[0])
useSQLOnClause = function_config.get('useSQLOnClause')

if useSQLOnClause:
    onClause = function_config.get('sql_on_clause')
else:
    onClause = """SELECT * FROM """ + function_config.get('input_table')


createTableQuery = """CREATE {tabletype} TABLE {searchPath}.{outputTable} AS (
SELECT *
FROM SCRIPT (ON ({onClause}){hashClause}{partitionClause}{orderClause}
             SCRIPT_COMMAND({script_command})
             RETURNS ('{returnClause}')
            )) WITH DATA {additionalClauses};""".\
            format(tabletype=function_config.get('table_type', ''),
                   searchPath=searchPath,
                   outputTable=outputTable,
                   onClause=onClause,
                   script_command=script_command,
                   hashClause=hashClause,
                   partitionClause=partitionClause,
                   orderClause=orderClause,
                   returnClause=returnClause,
                   additionalClauses=getAdditionalClauses(function_config.get('add_clauses','')))

            # RETURNS ('"""oc1 VARCHAR(10), oc2 VARCHAR(10), oc3 VARCHAR(18)"""')
# some helper function
def getFunctionQuery(inputDataset, outputDataset):
    return [installAdditionalFiles,
            setSessionQuery,
            etQuery,
            #installFileQuery,
            etQuery]

def getReplaceFunctionQuery():
    return [installAdditionalFiles,
            setSessionQuery,#Add UI option
            etQuery,
            # removeFileQuery,
            # etQuery,
            # installFileQuery,
            replaceFileQuery,
            etQuery]

            #Look for complex Orange book example
            #Tabs for input, output, and script

def getSelectTableQuery(inputDataset, inputTableName):
    return """select * from dbc.tables
where databasename = '{dataset}'
and TableName = '{table}'
and TableKind = 'T';""".format(dataset=inputDataset, table=inputTableName)

def getPassword():
    dbpwd = function_config.get('dbpwd', '')
    conn_name = getCurrentConnectionName(output_A_datasets[0])
    filepath = getAuthFilePath(conn_name)
    if dbpwd and function_config.get('savepwd', False):
        write_encrypted(filepath, dbpwd)
    else:
        dbpwd = read_encrypted(getAuthFilePath(conn_name))
    return dbpwd

def database():
    # for now, database name = db user name
    return db_user()
    
#PERFORM FILE LOADING
if function_config.get("replace_script"):
    bteqScript = """bteq << EOF 
              .LOGON 153.64.211.111/{user},{pwd};
              """.format(user=db_user(),pwd=getPassword()) +setSessionQuery+"""
              """+installAdditionalFiles+"""
              """+replaceFileQuery+"""
              .QUIT
              EOF"""
else:
    bteqScript = """bteq << EOF 
              .LOGON 153.64.211.111/{user},{pwd};
              """.format(user=db_user(),pwd=getPassword()) +setSessionQuery+"""
              """+installAdditionalFiles+"""
              """+installFileQuery+"""
              .QUIT
              EOF"""

try:
    exitValue = call([bteqScript],shell=True)
    if(exitValue !=0):
        print(exitValue)
        raise RuntimeError('Error during BTEQ Loading, please check logs for more information')
except Exception as error:
    print('Error during BTEQ File loading. Please check the logs')
    raise 


# actual query
print("Actual query")
query = getFunctionQuery(empty_table, None)
# query = getFunctionQuery(input_A_datasets[0], None)
print(query)
executor = SQLExecutor2(dataset=empty_table)
# executor = SQLExecutor2(dataset=input_A_datasets[0])

existingtable = executor.query_to_df(getSelectTableQuery(searchPath,
                                                         outputTable))
print(len(existingtable.index))
if len(existingtable.index):
    executor.query_to_df('COMMIT WORK',
                         ['DROP TABLE {searchPath}.{outputTable}'
                          .format(searchPath=searchPath,
                                  outputTable=outputTable)]);
# existingScript = executor.query_to_df(scriptDoesExist);
# if len(existingScript.index):
#     if function_config.get("replace_script"):
#         query = getReplaceFunctionQuery()
#     else:
#         query = getFunctionQuery(input_A_datasets[0], None)
# else:
#     query = getFunctionQuery(input_A_datasets[0], None) 
# lenquery = len(query) - 1
# executor.query_to_df(query[lenquery], query[-lenquery:])
executor.query_to_df('COMMIT WORK',
                     ['SET SESSION SEARCHUIFDBPATH = {searchPath};'.format(searchPath=searchPath),
                      createTableQuery])
#executor.query_to_df('\n'.join(query))

# Recipe outputs
nQuery = """SELECT * FROM {searchPath}.{table};""".format(searchPath=searchPath,
                                                          table=outputTable)
selectResult = executor.query_to_df(nQuery);
pythonrecipe_out = output_A_datasets[0]
pythonrecipe_out.write_with_schema(selectResult)
