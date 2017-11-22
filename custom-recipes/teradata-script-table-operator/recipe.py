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

# print('Starting handle')
handle = dataiku.Folder("sto_scripts") if input_B_names else None
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
print('Getting DB User')
def db_user():
    return getConnectionUser(output_A_datasets[0])
print('Getting Default Database')
def default_database(dataset=output_A_datasets[0]):
    return getConnectionParamsFromDataset(output_A_datasets[0]).get('defaultDatabase', "");

empty_table = input_A_datasets[0]

if default_database(input_A_datasets[0]) != default_database():
    raise RuntimeError('Input dataset and output dataset have different connection details')

output_location = output_A_datasets[0].get_location_info()['info']

print('Getting Function Config')
scriptAlias = function_config.get('script_alias', '')
scriptFileName = function_config.get('script_filename', '')
scriptLocation = function_config.get('script_location', '')
searchPath = default_database()
outputTable = output_A_datasets[0].get_location_info()['info'].get('table', '')


if(scriptLocation == 'sz'):
    scriptFileLocation = function_config.get('script_filelocation')
elif 'czp' == scriptLocation:
    scriptFileLocation = pynbDestinationPath(scriptFileName)
    writePythonNotebookToResourceFolder(output_A_names[0].split('.')[0], scriptFileName)
    scriptFileName = scriptFileName.replace("'", "")
    scriptFileName = scriptFileName.replace(" ", "")
else:
    scriptFileLocation = handle.file_path(scriptFileName)


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

def getAdditionalClauses(arg):
    return arg and ('\n{arg}'.format(arg=arg))

partitionClause = getPartitionClause(function_config.get('partitionby', ''))
hashClause = getHashClause(function_config.get('hashby', ''))
orderClause = getOrderClause(function_config.get('orderby', ''))

print('Building STO Script Command')
script_command = ''
if commandType == 'python':
    script_command = """'export PATH; python ./"""+searchPath+"""/"""+scriptFileName+""" """+scriptArguments+"""'"""
elif commandType == 'r':
    script_command = """'R --vanilla --slave -f ./"""+searchPath+"""/"""+scriptFileName+"""'"""
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
# installFileQuery = """CALL SYSUIF.INSTALL_FILE('""" + function_config.get('script_alias') + """','""" + function_config.get('script_filename') + """','cz!"""+filepath+"""');"""
print('Building Script installation query')
installFileQuery = """CALL SYSUIF.INSTALL_FILE('""" + escape(scriptAlias) + """','""" + escape(scriptFileName) + """','"""+escape(scriptLocation)+"""!"""+escape(scriptFileName)+"""');"""
#sz if in DB
replaceFileQuery = """CALL SYSUIF.REPLACE_FILE('""" + escape(scriptAlias) + """','""" + escape(scriptFileName) + """','"""+escape(scriptLocation)+"""!"""+escape(scriptFileName)+"""', 0);"""
scriptDoesExist = """select * from dbc.tables
where databasename = '{searchPath}'
and TableKind = 'Z';""".format(searchPath=searchPath)

#File Copy to DIST
dkuinstalldir = os.environ['DKUINSTALLDIR']
newPath = dkuinstalldir + """/dist/"""+scriptFileName
print(newPath)
print(replaceFileQuery)
#COPY FILE TEST
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
    print(replaceFileQuery)
    print(address)
    #COPY FILE TEST
    copyfile(address, newPath)
    if item.get('replace_file'):
        # installAdditionalFiles = installAdditionalFiles + """\nCALL SYSUIF.REPLACE_FILE('""" + item.get('file_alias') + """','""" + item.get('filename') + """','"""+item.get('file_location')+item.get('file_format')+"""!"""+address+"""',0);"""
        installAdditionalFilesArray.append("""\nCALL SYSUIF.REPLACE_FILE('""" + item.get('file_alias') + """','""" + item.get('filename') + """','"""+item.get('file_location')+item.get('file_format')+"""!"""+item.get('filename')+"""',0);""")
    else:
        # installAdditionalFiles = installAdditionalFiles + """\nCALL SYSUIF.INSTALL_FILE('""" + item.get('file_alias') + """','""" + item.get('filename') + """','"""+item.get('file_location')+item.get('file_format')+"""!"""+address+"""');"""
        installAdditionalFilesArray.append("""\nCALL SYSUIF.INSTALL_FILE('""" + item.get('file_alias') + """','""" + item.get('filename') + """','"""+item.get('file_location')+item.get('file_format')+"""!"""+item.get('filename')+"""');""")
print("""Additional Files Installation Query/ies: """)
print(installAdditionalFilesArray)

#MOVE ADDITIONAL FILES


print(output_A_names[0])
useSQLOnClause = function_config.get('useSQLOnClause')

# if useSQLOnClause:
onClause = function_config.get('sql_on_clause')
selectClause = function_config.get('select_clause')
# else:
    # onClause = """SELECT * FROM """ + function_config.get('input_table')


createTableQuery = """SELECT {selectClause}
FROM SCRIPT (ON ({onClause}){hashClause}{partitionClause}{orderClause}
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
                   returnClause=returnClause,
                   additionalClauses=getAdditionalClauses(function_config.get('add_clauses','')))


def removePasswordFromRecipe(projectname, outputdatasetname):
    client = dataiku.api_client()
    project = client.get_project(projectname)
    r = project.get_recipe("compute_{}".format(outputdatasetname))
    defr = r.get_definition_and_payload()
    if defr:
        params = defr.get_recipe_params()
        if 'customConfig' in defr.get_recipe_params() and 'function' in defr.get_recipe_params()['customConfig']:
            defr.get_recipe_params()['customConfig']['function'].pop('dbpwd', None)
            defr.get_recipe_params()['customConfig']['function'].pop('savepwd', None)
            r.set_definition_and_payload(defr)

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
    elif not dbpwd:
        dbpwd = read_encrypted(getAuthFilePath(conn_name))
    else:
        pass
    return dbpwd

def database():
    # for now, database name = db user name
    return default_database()
    
#PERFORM FILE LOADING
# print('BTEQ file loading starting...')
# if function_config.get("replace_script"):
#     bteqScript = """bteq << EOF 
#               .LOGON 153.64.211.111/{user},{pwd};
#               """.format(user=db_user(),pwd=getPassword()) +setSessionQuery+"""
#               """+databaseQuery+"""
#               """+installAdditionalFiles+"""
#               """+replaceFileQuery+"""
#               .QUIT
# EOF"""
# else:
#     bteqScript = """bteq << EOF 
#               .LOGON 153.64.211.111/{user},{pwd};
#               """.format(user=db_user(),pwd=getPassword()) +setSessionQuery+"""
#               """+databaseQuery+"""
#               """+installAdditionalFiles+"""
#               """+installFileQuery+"""
#               .QUIT
# EOF"""

# try:
#     exitValue = call([bteqScript],shell=True)
#     if(exitValue !=0):
#         print(exitValue)
#         raise RuntimeError('Error during BTEQ Loading, please check logs for more information')
# except Exception as error:
#     print('Error during BTEQ File loading. Please check the logs')
#     removePasswordFromRecipe(output_A_names[0].split('.')[0], output_A_names[0].split('.')[1])
#     raise 


# print('Performing nquery')
# selectResult = executor.query_to_df(nQuery,[setSessionQuery]);

# print('Results')
# print(selectResult)

# # actual query
# print("Actual query")
# query = getFunctionQuery(empty_table, None)
# # query = getFunctionQuery(input_A_datasets[0], None)
# print(query)
executor = SQLExecutor2(dataset=empty_table)



# if function_config.get("replace_script"):
if function_config.get("replace_script"):
    print('performing replacefile')
    executor.query_to_df(replaceFileQuery,[setSessionQuery]);
else:
    executor.query_to_df(installFileQuery,[setSessionQuery]);

if(installAdditionalFilesArray != []):
    print('Installing additional files...')
    executor.query_to_df(installAdditionalFilesArray,[setSessionQuery])
    
# executor = SQLExecutor2(dataset=input_A_datasets[0])

#November 20, 2017 3:55PM REMOVING TO MOVE TO SELECT ONLY
# print('Checking for existing table')
# existingtable = executor.query_to_df(getSelectTableQuery(searchPath,
#                                                          outputTable))

# print('Existing Tables:')
# print(len(existingtable.index))
# if len(existingtable.index):
#     print('Dropping tables')
#     executor.query_to_df('COMMIT WORK',
#                          ['DROP TABLE {searchPath}.{outputTable}'
#                           .format(searchPath=searchPath,
#                                   outputTable=outputTable)]);


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

#Removing for now
# print('Creating table...')
# print("""Create table statement: """+createTableQuery)
# executor.query_to_df('COMMIT WORK;',
#                      ['SET SESSION SEARCHUIFDBPATH = {searchPath};'.format(searchPath=searchPath),
#                       databaseQuery,
#                       'COMMIT WORK;',
#                       createTableQuery])
#executor.query_to_df('\n'.join(query))

# Recipe outputs
print('Preparing SELECT Query for DSS Results...')
# nQuery = """SELECT * FROM {searchPath}.{table};""".format(searchPath=searchPath,
#                                                           table=outputTable)
                                                          
print('setSessionQuery')
print(setSessionQuery)
print('replaceFileQuery')
print(replaceFileQuery)
# print("""SELECT Query: """+nQuery)
print('Executing SELECT Query...')
selectResult = executor.query_to_df(createTableQuery,[setSessionQuery])
print('Moving results to output...')
pythonrecipe_out = output_A_datasets[0]
pythonrecipe_out.write_with_schema(selectResult)
# print('Cleaning password from Recipe')
# removePasswordFromRecipe(output_A_names[0].split('.')[0], output_A_names[0].split('.')[1])
print('Complete!')