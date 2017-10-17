'''
Created on 12 Oct 2017

@author: dt186022
'''

import os
import dataiku
from dataiku.customrecipe import *

DATA_DIR = "/home/dataiku/dss_data/"
PYNBDIR = "config/ipython_notebooks/"
PLUGINS_DIR = "plugins/installed"

CELLS = 'cells'
SOURCE = 'source'
CODE = 'code'
CELL_TYPE = 'cell_type'

def escape(s):
    return s.replace("'", "''")

def readfile(pynbpath):
    data = {}
    with open(pynbpath, 'r') as file:
        data=json.loads(file.read())
    return data

def writefile(destinationpath, lines):
    with open(destinationpath, 'w') as f:
        f.write('\n'.join(lines))

def pynbDestinationPath(pynb):
    return os.path.join(get_recipe_resource(), pynb).replace("'", "").replace(" ", "")

def writePythonNotebookToResourceFolder(project, pynb):
    pynbpath = os.path.join(DATA_DIR, PYNBDIR, project, pynb)
    # get cells from Python notebook
    cells = readfile(pynbpath).get(CELLS, [])
    # read the lines of codes in each cell
    sourcecodes = [cell.get(SOURCE, '') for cell in cells if CODE == cell.get(CELL_TYPE, '')]
    flattenedsource = [line for lines in sourcecodes for line in lines]
    writefile(pynbDestinationPath(pynb), flattenedsource)
    

def writePythonNotebooksToResourceFolder(listOfPythonNbs, project):
    for pynb in listOfPythonNbs:
        writePythonNotebookToResourceFolder(project, pynb)
    
