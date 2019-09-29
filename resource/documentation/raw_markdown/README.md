# I. Introduction

---

Dataiku Data Science Studio (DSS) is a collaborative platform that enables teams of people with different data expertise, such as data engineers, data scientists and analysts, to work together efficiently.  Dataiku DSS provides a set of built-in recipes or operations that can be applied to transform or analyze a dataset.  It also allows users to create their own recipes in Python, SQL or R.  The DSS plugins are custom reusable recipes that can only be written in Python.

The present guide outlines installation and usage of 2 DSS plugins that enable you to interact with Teradata Vantage systems; namely, the Teradata Vantage Analytic Functions Plugin, and the SCRIPT Table Operator Plugin.

##Teradata Vantage Analytic Functions Plugin

The Teradata Vantage Analytic Functions Plugin for Dataiku DSS integrates about 180 of the Vantage Machine Learning Engine (MLE) analytic functions, by providing a user-friendly, easy-to-use, no-SQL interface for the functions in the Dataiku DSS environment. The Vantage analytic functions can be accessed through the \[`+RECIPE`\] menu of the FLOW view of a Dataiku project, and are grouped into nine categories:

* Time Series, Path and Attribution Analysis
* Ensemble Methods
* Text Analysis
* Naïve Bayes
* Graph Analysis
* Association Analysis
* Statistical Analysis
* Cluster Analysis
* Data Transformation

The Teradata Vantage Analytic Functions Plugin for Dataiku DSS integrates about 180 of the Vantage Machine Learning Engine (MLE) analytic functions, by providing a user-friendly, easy-to-use, no-SQL interface for the functions in the Dataiku DSS environment. The Vantage analytic functions can be accessed through the [+RECIPE] menu of the FLOW view of a Dataiku project, and are grouped into nine categories:

**SCRIPT Table Operator Plugin**

The SCRIPT Table Operator Plugin allows the execution of R or Python scripts inside the Teradata Database. The plugin will take an R or Python script within a DSS notebook, or an R or Python script uploaded to the plugin and  install  the scripts and other related files (i.e. saved models in RDS or pickle files) on the Advanced SQL Engine.

Similar to the Teradata Vantage Analytic Functions Plugin, the SCRIPT Table Operator Plugin translates the user-requested tasks in the plugin into SQL queries, which are then sent to a connected Vantage system to set up and invoke the SCRIPT Table Operator.

