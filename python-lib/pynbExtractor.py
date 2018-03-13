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
Created on 12 Oct 2017

@author: dt186022
'''

import os
import dataiku
from dataiku.customrecipe import *


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
    DATA_DIR = os.environ["DIP_HOME"]
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
    
