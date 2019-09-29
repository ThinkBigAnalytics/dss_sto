# III. Creating a Vantage Connection

---

1.    Follow the instructions in the Dataiku Reference Document for Installing Database Drivers. In summary, one needs to execute from the command line of a DSS server:
    
        a.    Stop the Data Science Studio server, where `DATA_DIR` is the data directory where Data Science Studio is installed:
        
        &nbsp;    `DATA_DIR/bin/dss stop`
        
        b.    Copy the Teradata JDBC driver to the `DATA_DIR/lib/jdbc` directory.  
        &nbsp;    
        
        c.    Restart Data Science Studio        
        &nbsp;    `DATA_DIR/bin/dss start`
        
2.    Access Dataiku DSS on a browser. Then, on the Dataiku DSS home page click on Apps.         

    ![](/assets/1a.png)
&nbsp;
Then, on the submenu click \[`Administration`\] (gear icon).
&nbsp;
    ![](/assets/1b.png)
    Alternatively, you can go to `http://dataikuhost:port/admin/`.
    
3.    On the DSS settings page, go to the \[`Connections`\] tab. Click on [`NEW CONNECTION`]. Choose \[`Teradata`\] among the options that will be presented.

    ![](/assets/2.png)
    
4.    Fill up the fields as needed:    
        **Basic Params**
        **Host**: <`database.host.name`>  
        **User**: <`Username`>         
        **Password**: <`User_password`>  
        **Default Database**: <`default_database`>  
        **Advanced JDBC properties**:  
	    `CHARSET: UTF8`  
	    `TMODE: TERA`  
        
        All other fields can be left as-is.

5.    Modify “Details readable by” to either `Every Analyst` or `Selected Groups`. 	
&nbsp;
![](/assets/connection_security.png)

6.    Click on the \[`Test`\] button to verify that connection details provided are valid.
7.    Finally, click on the \[`Save`\] button.
