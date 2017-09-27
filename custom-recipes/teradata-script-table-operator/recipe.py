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
filepath = "/home/aagdcph/ex2p.py"
# filepath ="/home/dataiku/dss_data/managed_folders/DT186022_TEST/kA2too62/"
print(filepath) 
# path = handle.get_path()

# print('Check folder directory')
# print(handle)
# print(path)
# print("Stuff")
# print(os.listdir("/home/dataiku/dss_data/managed_folders/DT186022_TEST/kA2too62/"))


# print(jsonfile)
# Recipe inputs
# empty_table = dataiku.Dataset("empty_table")
empty_table = input_A_datasets[0]
print("Location info")
output_location = output_A_datasets[0].get_location_info()['info']
print(output_location)
# print(output_A_datasets[0].get_location_info()['info'])
# empty_table_df = empty_table.get_dataframe()

print("Past empty table")

scriptAlias = function_config.get('script_alias')
scriptFileName = function_config.get('script_filename')
scriptLocation = function_config.get('script_location')

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

partitionClause = getPartitionClause(function_config.get('partitionby', ''))
hashClause = getHashClause(function_config.get('hashby', ''))
orderClause = getOrderClause(function_config.get('orderby', ''))

database = 'aagdcph'
script_command = ''
if commandType == 'python':
    script_command = """'export PATH; python ./"""+database+"""/"""+scriptFileName+""" """+scriptArguments+"""'"""
elif commandType == 'r':
    script_command = """'R --vanilla --slave -f ./"""+database+"""/"""+scriptFileName+"""'"""

#INSTALL Additional files
installAdditionalFiles = """"""
for item in additionalFiles:
    if item.get('replace_file'):
        installAdditionalFiles = installAdditionalFiles + """\nCALL SYSUIF.REPLACE_FILE('""" + item.get('file_alias') + """','""" + item.get('filename') + """','"""+item.get('file_location')+item.get('file_format')+"""!"""+item.get('file_address').rstrip()+"""',0);"""        
    else:
        installAdditionalFiles = installAdditionalFiles + """\nCALL SYSUIF.INSTALL_FILE('""" + item.get('file_alias') + """','""" + item.get('filename') + """','"""+item.get('file_location')+item.get('file_format')+"""!"""+item.get('file_address').rstrip()+"""');"""

# select query
setSessionQuery = 'SET SESSION SEARCHUIFDBPATH = aagdcph;'
etQuery = 'COMMIT WORK;'
removeFileQuery = """CALL SYSUIF.REMOVE_FILE('""" + scriptAlias + """',1);"""
# installFileQuery = """CALL SYSUIF.INSTALL_FILE('""" + function_config.get('script_alias') + """','""" + function_config.get('script_filename') + """','cz!"""+filepath+"""');"""
installFileQuery = """CALL SYSUIF.INSTALL_FILE('""" + scriptAlias + """','""" + scriptFileName + """','"""+scriptLocation+"""!"""+scriptFileLocation.rstrip()+"""');"""

#sz if in DB
replaceFileQuery = """CALL SYSUIF.REPLACE_FILE('""" + scriptAlias + """','""" + scriptFileName + """','"""+scriptLocation+"""!"""+scriptFileLocation.rstrip()+"""', 0);"""
createOutputTableQuery = """CREATE TABLE aagdcph.DT186022_TEST_pythonrecipe_out AS (
SELECT COUNT(*) AS nSims,
       AVG(CAST (oc1 AS INT)) AS AvgCustomers, 
       AVG(CAST (oc2 AS INT)) AS AvgReneged,
       AVG(CAST (oc3 AS FLOAT)) AS AvgWaitTime
FROM SCRIPT (ON (SELECT * FROM ex2tbl) 
             SCRIPT_COMMAND('export PATH; """ + commandType + """ ./aagdcph/"""+scriptFileName+""" 4 5 10 6 480')
             RETURNS ('oc1 VARCHAR(10), oc2 VARCHAR(10), oc3 VARCHAR(18)')
            )) WITH DATA;"""
scriptDoesExist = """select * from dbc.tables
where databasename = 'aagdcph'
and TableKind = 'Z';"""
insertIntoOutputTableQuery = """select 1 from dbc.Tables where databasename LIKE 'AAGDCPH' and TableName = 'DT186022_TEST_pythonrecipe_out';
IF ACTIVITYCOUNT = 0 THEN .GOTO ok;
DROP TABLE aagdch.DT186022_TEST_pythonrecipe_out;
.label ok
CREATE SET TABLE aagdcph.DT186022_TEST_pythonrecipe_out
( EmpID INT,
 EmpName VARCHAR(20)
);
INSERT INTO aagdcph.DT186022_TEST_pythonrecipe_out
SELECT COUNT(*) AS nSims,
       AVG(CAST (oc1 AS INT)) AS AvgCustomers, 
       AVG(CAST (oc2 AS INT)) AS AvgReneged,
       AVG(CAST (oc3 AS FLOAT)) AS AvgWaitTime
FROM SCRIPT (ON (SELECT * FROM ex2tbl) 
             SCRIPT_COMMAND("""'export PATH; """ + commandType + """ ./aagdcph/ex2p.py 4 5 10 6 480'""")
             RETURNS ('oc1 VARCHAR(10), oc2 VARCHAR(10), oc3 VARCHAR(18)')
            );"""
print("Output names")
print(output_A_names[0])
useSQLOnClause = function_config.get('useSQLOnClause')

if useSQLOnClause:
    onClause = function_config.get('sql_on_clause')
else:
    onClause = """SELECT * FROM """ + function_config.get('input_table')


createTableQuery = """CREATE TABLE aagdcph.DT186022_TEST_pythonrecipe_out AS (
SELECT *
FROM SCRIPT (ON ({onClause}) 
             SCRIPT_COMMAND({script_command}){hashClause}{partitionClause}{orderClause}
             RETURNS ('{returnClause}')
            )) WITH DATA;""".format(onClause=onClause,
                                    script_command=script_command,
                                    hashClause=hashClause,
                                    partitionClause=partitionClause,
                                    orderClause=orderClause,
                                    returnClause=returnClause)

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
where databasename = {dataset}
and TableName = {table}
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

db_user = getConnectionUser(output_A_datasets[0])
    
#PERFORM FILE LOADING
if function_config.get("replace_script"):
    bteqScript = """bteq << EOF 
              .LOGON 153.64.211.111/{user},{pwd};
              """.format(user=db_user,pwd=getPassword()) +setSessionQuery+"""
              """+installAdditionalFiles+"""
              """+replaceFileQuery+"""
              .QUIT
              EOF"""
else:
    bteqScript = """bteq << EOF 
              .LOGON 153.64.211.111/{user},{pwd};
              """.format(user=db_user,pwd=getPassword()) +setSessionQuery+"""
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

existingtable = executor.query_to_df(getSelectTableQuery("'aagdcph'", "'DT186022_TEST_pythonrecipe_out'"))
print(len(existingtable.index))
if len(existingtable.index):
    executor.query_to_df('COMMIT WORK',['DROP TABLE aagdcph.DT186022_TEST_pythonrecipe_out']);
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
executor.query_to_df('COMMIT WORK', ['SET SESSION SEARCHUIFDBPATH = aagdcph;', createTableQuery])
#executor.query_to_df('\n'.join(query))

# Recipe outputs
nQuery = """SELECT * FROM {};""".format('aagdcph.DT186022_TEST_pythonrecipe_out')
selectResult = executor.query_to_df(nQuery);
pythonrecipe_out = output_A_datasets[0]
pythonrecipe_out.write_with_schema(selectResult)

