-------------------------------------------------------------------------
- README file: The Teradata Vantage Analytic Functions Plugin & 
-              The Teradata Vantage SCRIPT Table Operator Plugin for R
-              and Python
-              
- Version: 1.0
-
- September 2019                                                                   
-
- Copyright (c) 2019 Teradata
-------------------------------------------------------------------------

The Teradata Vantage Plugins for Dataiku Data Science Studio (DSS) allows 
end users to leverage Vantage analytics within their DSS data science workflows.
The first, the Teradata Vantage Analytic Functions Plugin for Dataiku DSS, 
supports all 180+ Vantage Machine Learning and Graph Engine functions as well 
as the new analytic functions built into the Advanced SQL engine in 16.20.  

The second, the Teradata Vantage SCRIPT Table Operator Plugin for R and Python, 
allows the end user to include custom built R or Python code from notebooks built
in Dataiku DSS that will push down and execute directly in the Advanced SQL engine of 
the Teradata Vantage platform through the built-in table operator, SCRIPT.

Note that Dataiku DSS itself also supports ANSI SQL push-down for most of their 
data preprocessing Visual recipes.  

The Plugins were tested with Vantage 1.1 Release Candidate 9.

I. System Requirements
----------------------

The following component versions are required for the Teradata Vantage Plugin: 

1. Dataiku Data Science Studio version 5.1
2. Teradata Vantage 1.1
3. Teradata JDBC Driver 16.20

For R and Python support in Teradata Vantage, one or both of the following are required:

1. 9687-2000-0120	R Interpreter and Add-on Pkg on Teradata Advanced SQL
2. 9687-2000-0122	Python Interpreter and Add-on Pkg on Teradata Advanced SQL

II. Install / Upgrade Instructions
----------------------------------

In order to install the Teradata Vantage Plugins for Dataiku DSS, perform the following:

1. In DSS Settings page (accessible through the Admin Tools button),
   select the [Plugins] tab, then select the [ADVANCED] option.
2. Click on [Choose File] and browse to the location of the Teradata
   Vantage Analytic Functions plugin zip file in your local filesystem.
3. If a previous installation of the Teradata Vantage Analytic plugin 
   exists, check "Is update".
4. Click on [UPLOAD] button.
5. When the upload succeeds, click on [Reload] button, or do a hard 
   refresh (Ctrl + F5) on all open Dataiku browsers for the change to 
   take effect.
6. Repeat for the Teradata Vantage SCRIPT Table Operator Plugin for R and 
   Python.


III. Limitations
----------------

1. For analytic functions that:
   - take in output table names as arguments, and
   - where the select query produces only a message table indicating the name 
     of the output model/metrics table, it is the responsibility of the user 
	 to specify output table names that are different from those of the existing
	 tables.
   Some analytic functions provide an option to delete an already existing output
   table prior to executing an algorithm, but others do not. In the former case, 
   the Advanced SQL Engine throws an "Already exists" exception.

2. The appended version of the Dataiku DSS Teradata Vantage Analytic Functions 
   plugin has been tested to work with the MLE analytic functions on Vantage 1.1 
   systems. Earlier or later analytic function versions may require a different 
   set of function metadata.

3. The plugin currently only supports Advanced SQL Engine datasets as input and output.

4. Functions with any OUTPUT TABLE type arguments will require the user to add an 
   output dataset for the SELECT statement results of the query and any additional
   output tables. Please refer to the Teradata Vantage Machine Learning Engine 
   Analytic Function Reference documentation page at docs.teradata.com to learn 
   about the output tables of each function.

5. MapReduce Function pairs are currently limited to the following select few: 
   - ApproxDCount, 
   - ApproxPercentile, 
   - Correlation, 
   - PCA,
   - Naïve Bayes. 
   In order to use these functions, please call their corresponding Map Functions 
   on the function selection box and it will display the arguments for both functions.

6. The following issues are still open at the time of the release of the plugin:

|-------------------------------------|---------------------------------------|------|
| Functional Category - Function Name |             Issue                     | JIRA |
|-------------------------------------|---------------------------------------|------|
| Text - AnalysisTextTokenizer        | Chinese characters might be causing   | N/A  |
|                                     | the plugin to fail.  The same function|      |
|                                     | runs in other query tools (Teradata   |      |
|                                     | Studio).                              |      |
|-------------------------------------|---------------------------------------|------|
| Statistical Analysis -              | Function requires a different         | ANLY-|
| ApproxPercentileReduce              | requiredInputKind than PartitionByAny | 8404 |
|                                     | APPROXPERCENTILEREDUCE cannot be used |      |
|                                     | as a row function.                    |      |
|-------------------------------------|---------------------------------------|------|
| Time Series - TimeSeriesOrders      | An error is encountered when executing| ANLY-|
|                                     | the TimeSeriesOrders Function using   | 8400 |
|                                     | either the plugin or the Teradata     |      |
|                                     | studio. The same error is encountered |      |
|                                     | for both tools.“PARTITION BY 1” is not|      |
|                                     | supported by the Metadata – error 9134|      |
|                                     | returned.                             |      |
|-------------------------------------|---------------------------------------|------|
| Time Series -                       | Column (required arguments) refers to | ANLY-|
| ShapeletSupervisedResponse          | a table that is an Optional Argument. | 8649 |
|                                     | It is quite misleading to the user.   |      |
|                                     | Based on the Vantage documentation,   |      |
|                                     | the Reponse Column should refer to the|      |
|                                     | Inputtable as well. This is a Metadata|      |
|                                     | BUG.                                  |      |    
|-------------------------------------|---------------------------------------|------|
| Text Analysis -                     | These functions are not merged. We may| ANLY-|
| NamedEntityFinderEvaluatorMap/Reduce| need to confirm if this is expected.  | 8650 |
|                                     | We will proceed with the unmerged     |      |
|                                     | function for now. We have implemented |      |
|                                     | merged functions when partner_function|      |
|                                     | is set in the metadata. However, for  |      |
|                                     | the affected functions, they are not  |      |
|                                     | set as partner_function.              |      |
|-------------------------------------|---------------------------------------|------|
| Statistical Analysis -              | Examples show that CoxHazardRatio's   | ANLY-|
| CoxHazardRatio                      | input table PredictorValues should    | 8400 |
|                                     | have partitionByOne and perhaps       |      |
|                                     | partitionByOneInclusive in the new    |      |
|                                     | JSON metadata. The JSON for           |      |
|                                     | CoxHazardRatio might need to be       |      |
|                                     | updated to set partitionByOne to TRUE |      |
|                                     | and partitionByOneInclusive to TRUE.  |      |
|-------------------------------------|---------------------------------------|------|
| Statistical Analysis -              | Unable to match example SQL due to UI | DOC  |
| CrossValidation                     | not supporting such an action. The    |      |
|                                     | argument "LinkFunction" for           |      |
|                                     | CrossValidation's interface does not  |      |
|                                     | support accepting multiple similar    |      |
|                                     | values.  This may also be an issue for|      |
|                                     | other functions which have            |      |
|                                     | "permittedValues" and "allowsList"    |      |
|                                     | in the argument's properties.         |      | 
|-------------------------------------|---------------------------------------|------|
| Statistical Analysis -              | Unable to Replicate Query, function   | ANLY-|
| DistributionMatchReduce             | requires partitionByOne and perhaps   | 8685 |
|                                     | partitionByOneInclusive to be true.   |      |
|-------------------------------------|---------------------------------------|------|
| Time Series - ARIMA                 | Input table and orders table does not |  ?   |
|                                     | have PARTITION BY Clause in the UI.   |      |
|-------------------------------------|---------------------------------------|------|
| Ensemble - AdaBoostPredict          | ORDER BY clause for the Model table is|  ?   |
|                                     | set to Required Arguments even though |      |
|                                     | they are Optional Arguments.          |      |
|-------------------------------------|---------------------------------------|------|
 
 
  Therefore, usage of certain functions are currently restricted. Those functions include:
	o Statistical Analysis 
		• Approximate Percentile Map/Reduce
		• Correlation Map/Reduce
		• Cox Hazard Ratio
		• Cross Validation
		• Distribution Match Reduce
	o Text Analysis 
		• Text Tokenizer
		• Named Entity Finder Evaluator Map/Reduce
	o Time Series 
		• Time Series Orders
		• Shapelet Supervised
		• ARIMA
		• VARMAX 5 and 6 (due to dependencies on Time Series Orders)
	o Ensemble 
		• AdaBoost Predict

7. References
-------------

For additional information on the Teradata Vantage Analytic Functions search for the following on docs.teradata.com:

1. "Teradata Vantage SQL Operators and User-Defined Functions"
2. "Teradata Vantage User Guide"
3. "Teradata Vantage Analytic Function User Guide"
4. "Teradata Vantage - Advanced SQL Engine Analytic Functions"
5. "Teradata Vantage Machine Learning Engine Analytic Function Reference"
