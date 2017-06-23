"""

Campaign optimization step 1

Prerequisites:
  - SQL Server 2017 with Python Services installed
  - SQL Server Management Studio
  - A database created called 'Campaign' (use SSMS to create)
  - Visual Studio 2015/2017 or Visual Studio Code or similar code editor
  - Python path or Path env variables set to Python Services python or
      a virtual environment with Python Services python version
      (set system Path variable and add to Visual Studio Python
      Environments or create a virtual environment on the command line
      with 'virtualenv <python version path>' and 'activate' to begin using)
  - Command line or equivalent access

What this script does:
* Reads data into dataframe and dummies categorical variables
* Imports dataframe to SQL Server table in established database
* In-dabase processing and linear regression with revoscalepy
* In-memory linear regression with scikit-learn

TODO: compare in-database to local compute context

* Report metrics
* Visualize with Python plotting tools

Micheleen Harris
"""

from revoscalepy.computecontext.RxComputeContext import  RxComputeContext
from revoscalepy.computecontext.RxInSqlServer import RxSqlServerData, RxInSqlServer
from revoscalepy.etl.RxImport import rx_import_datasource
# from revoscalepy.etl.RxImport import RxDataSource
# from revoscalepy.functions.RxDataStep import rx_data_step_ex
from revoscalepy.functions.RxLinMod import rx_lin_mod_ex
# from revoscalepy.functions.RxLogit import rx_logit_ex
from revoscalepy.functions.RxPredict import rx_predict_ex
from revoscalepy.functions.RxSummary import rx_summary
# from revoscalepy.utils.RxOptions import RxOptions
from sklearn.linear_model import LinearRegression

from config import CONNECTION_STRING, BASE_DIR
import os
import pandas as pd
from pprint import pprint

def main(tablename, inputdf, overwrite=False):
    """Imports a DataFrame into SQL Server table and, operating
       in-database, uses linear regression to create a
       predictive model.

       A comparison to an out-of-database, in memory method
       is performed.
       
       Parameters
       ----------

       tablename : str
           The new or previosly create table name in database.

       inputdf : pandas.DataFrame
           The DataFrame with data for import.

       overwrite : bool
           Whether or not to overwrite the table.  
           Set to True if this is a new table.
       """

    ####################################################################
    # Set the compute context to SQL SERVER
    ####################################################################

    # NB: don't need, but would be good to know what this actually does here
    # RxComputeContext(LOCAL, '9.1')

    compute_context = RxInSqlServer(
        connectionString = CONNECTION_STRING,
        numTasks = 1,
        autoCleanup = False
        )

    if overwrite:

        ####################################################################
        # Create table in SQL server
        ####################################################################

        print("Creating tables...")
        outfile = RxSqlServerData(
            table = tablename, 
            connectionString = CONNECTION_STRING)

        ####################################################################
        # Read data into the SQL server table that was just created
        ####################################################################

        print("Reading data into tables...")

        rx_import_datasource(inData=inputdf, \
            outFile=outfile)

        # Right now can only run once because overwrite is not working
        # rx_import_datasource(inData=inputdf, \
        #     outFile=outfile, overwrite=True)

        # NB: overwrite param not accepting bool values so this can only be run once righ now!

    #####################################################################
    # Set up a query on table for train and test data (and ensure factor levels)
    #####################################################################

    # Train data
    data_source_train = RxSqlServerData(
        sqlQuery = "SELECT TOP 10000 * FROM Lead_Demography_Tbl \
                    ORDER BY Lead_Id", 
        connectionString = CONNECTION_STRING,
        colInfo = { # NB: may want to add all cols here
            "No_Of_Children" : { "type" : "integer" },
            "Household_Size" : { "type" : "integer" },
            "No_Of_Dependents" : { "type" : "integer" }
            }
        )

    # Import training data RxImport style from new query source       
    X_y_train = rx_import_datasource(data_source_train)
    # print(X_y_train)

    # Test data (let's pick ~30% size of training dataset)
    data_source_test = RxSqlServerData(
        sqlQuery = "SELECT * FROM Lead_Demography_Tbl \
                    ORDER BY Lead_Id \
                    OFFSET 10000 ROWS \
                    FETCH FIRST 3000 ROW ONLY", 
        connectionString = CONNECTION_STRING,
        colInfo = { # NB: may want to add all cols here
            "No_Of_Children" : { "type" : "integer" },
            "Household_Size" : { "type" : "integer" },
            "No_Of_Dependents" : { "type" : "integer" }
            }
        )

    # Import data RxImport style from new query source       
    X_y_test = rx_import_datasource(data_source_test)

    #####################################################################
    # Run revoscalepy linear regression and summary (in-database)
    #####################################################################

    mod = rx_lin_mod_ex(formula="No_Of_Children ~ \
                                F(Highest_Education_High_School)+\
                                F(Annual_Income_Bucket_lt60k)", 
        data=X_y_train, compute_context=compute_context)
    assert mod is not None
    assert mod._results is not None
    pprint(mod._results)
    summary = rx_summary(formula="No_Of_Children ~ \
                                F(Highest_Education_High_School)+\
                                F(Annual_Income_Bucket_lt60k)", 
        data=X_y_train, compute_context=compute_context)
    # print(summary) # NB: Doesn't want to print
    
    #####################################################################
    # Run scikit-learn linear regression (in-memory)
    #####################################################################

    df_train = pd.DataFrame(X_y_train)
    df_test = pd.DataFrame(X_y_test)

if __name__ == '__main__':


    ####################################################################
    # Read in data into a pandas df from a file (do here for manipulation)
    ####################################################################

    file_path = os.path.join(BASE_DIR, 'Data')
    inputfile = os.path.join(file_path, "Lead_Demography.csv")

    # TODO:  look into dask for holding chunks of data for import
    # Create the file path to the csv data
    input_df = pd.read_csv(inputfile)

    ####################################################################
    # Dummy encode variables of interest
    ####################################################################

    df_dummy = pd.get_dummies(input_df, 
        columns=['Annual_Income_Bucket', 'Highest_Education'])

    # Fix column names
    new_cols = [x.replace(' ', '_').replace('>', 'gt').replace('<', 'lt') 
        for x in df_dummy.columns]
    print(new_cols)
    df_dummy.columns = new_cols

    main(tablename="Lead_Demography_Tbl", inputdf=df_dummy, 
        overwrite=True)