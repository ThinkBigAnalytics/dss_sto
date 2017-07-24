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

# Inputs and outputs are defined by roles. In the recipe's I/O tab, the user can associate one
# or more dataset to each input and output role.
# Roles need to be defined in recipe.json, in the inputRoles and outputRoles fields.

# To  retrieve the datasets of an input role named 'input_A' as an array of dataset names:
input_A_names = get_input_names_for_role('main')
# The dataset objects themselves can then be created like this:
input_A_datasets = [dataiku.Dataset(name) for name in input_A_names]

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
from dataiku import pandasutils as pdu
from dataiku.core.sql import SQLExecutor2

print('Checks')
print(function_config)
print(output_A_names)
print(output_A_datasets)
# Recipe inputs
empty_table = dataiku.Dataset("empty_table")
empty_table_df = empty_table.get_dataframe()

# select query
setSessionQuery = 'SET SESSION SEARCHUIFDBPATH = aagdcph;'
etQuery = 'COMMIT WORK;'
removeFileQuery = """CALL SYSUIF.REMOVE_FILE('""" + function_config.get('script_alias') + """',1);"""
installFileQuery = """CALL SYSUIF.INSTALL_FILE('""" + function_config.get('script_alias') + """','""" + function_config.get('script_filename') + """','sz!/home/aagdcph/ex2p.py');"""
replaceFileQuery = """CALL SYSUIF.REPLACE_FILE('""" + function_config.get('script_alias') + """','""" + function_config.get('script_filename') + """','sz!/home/aagdcph/ex2p.py');"""
createOutputTableQuery = """CREATE TABLE aagdcph.DT186022_TEST_pythonrecipe_out AS (
SELECT COUNT(*) AS nSims,
       AVG(CAST (oc1 AS INT)) AS AvgCustomers, 
       AVG(CAST (oc2 AS INT)) AS AvgReneged,
       AVG(CAST (oc3 AS FLOAT)) AS AvgWaitTime
FROM SCRIPT (ON (SELECT * FROM ex2tbl) 
             SCRIPT_COMMAND('export PATH; """ + function_config.get('command_type') + """ ./aagdcph/ex2p.py 4 5 10 6 480')
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
             SCRIPT_COMMAND('export PATH; """ + function_config.get('command_type') + """ ./aagdcph/ex2p.py 4 5 10 6 480')
             RETURNS ('oc1 VARCHAR(10), oc2 VARCHAR(10), oc3 VARCHAR(18)')
            );"""
createTableQuery = """CREATE TABLE aagdcph.DT186022_TEST_pythonrecipe_out AS (
SELECT COUNT(*) AS nSims,
       AVG(CAST (oc1 AS INT)) AS AvgCustomers, 
       AVG(CAST (oc2 AS INT)) AS AvgReneged,
       AVG(CAST (oc3 AS FLOAT)) AS AvgWaitTime
FROM SCRIPT (ON (SELECT * FROM ex2tbl) 
             SCRIPT_COMMAND('export PATH; """ + function_config.get('command_type') + """ ./aagdcph/ex2p.py 4 5 10 6 480')
             RETURNS ('oc1 VARCHAR(10), oc2 VARCHAR(10), oc3 VARCHAR(18)')
            )) WITH DATA;"""
# some helper function
def getFunctionQuery(inputDataset, outputDataset):
    return [setSessionQuery,
            etQuery,
            installFileQuery,
            etQuery]

def getReplaceFunctionQuery():
    return [setSessionQuery,
            etQuery,
            removeFileQuery,
            etQuery,
            installFileQuery,
            etQuery]

def getSelectTableQuery(inputDataset, inputTableName):
    return """select * from dbc.tables
where databasename = {dataset}
and TableName = {table}
and TableKind = 'T';""".format(dataset=inputDataset, table=inputTableName)
    

# actual query
query = getFunctionQuery(empty_table, None)
print(query)
executor = SQLExecutor2(dataset=empty_table)

existingtable = executor.query_to_df(getSelectTableQuery("'aagdcph'", "'DT186022_TEST_pythonrecipe_out'"))
print(len(existingtable.index))
if len(existingtable.index):
    executor.query_to_df('COMMIT WORK',['DROP TABLE aagdcph.DT186022_TEST_pythonrecipe_out']);
existingScript = executor.query_to_df(scriptDoesExist);
if len(existingScript.index):
    query = getReplaceFunctionQuery()
else:
    query = getFunctionQuery(empty_table, None) 
lenquery = len(query) - 1
executor.query_to_df(query[lenquery], query[-lenquery:])
executor.query_to_df('COMMIT WORK', ['SET SESSION SEARCHUIFDBPATH = aagdcph;', createTableQuery])
#executor.query_to_df('\n'.join(query))

# Recipe outputs
nQuery = """SELECT * FROM {} SAMPLE 10;""".format('aagdcph.DT186022_TEST_pythonrecipe_out')
selectResult = executor.query_to_df(nQuery);
pythonrecipe_out = output_A_datasets[0]
pythonrecipe_out.write_with_schema(selectResult)

