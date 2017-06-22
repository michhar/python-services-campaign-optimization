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

What these scripts do:
* Reads data into dataframe
* Imports dataframe to SQL Server Campaign database
* Processes in-database with revoscalepy stats and ML methods
* Report metrics
* Visualize with Python plotting tools

Micheleen Harris
"""

from revoscalepy.computecontext.RxComputeContext import  RxComputeContext
from revoscalepy.computecontext.RxInSqlServer import RxSqlServerData, RxInSqlServer
from revoscalepy.etl.RxImport import rx_import_datasource
# from revoscalepy.etl.RxImport import RxDataSource
from revoscalepy.functions.RxDataStep import rx_data_step_ex
from revoscalepy.functions.RxLinMod import rx_lin_mod_ex
from revoscalepy.functions.RxLogit import rx_logit_ex
from revoscalepy.functions.RxPredict import rx_predict_ex
from revoscalepy.functions.RxSummary import rx_summary
from revoscalepy.utils.RxOptions import RxOptions
from sklearn.linear_model import LinearRegression

from config import CONNECTION_STRING, BASE_DIR, LOCAL
import os
import pandas as pd
from pprint import pprint

def main(tablename, csvfile, overwrite=False):
    """Imports a csv into SQL Server table and performs linear 
       regression test.
       
       Parameters
       ----------

       tablename : str
           The new or previosly create table name in database.

       csvfile : str
           The csv file with data for import (full path).

       overwrite : bool
           Whether or not to overwrite the table.  Set to
           True if this is a new table.
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
        # Read in data into a pandas df from a file
        ####################################################################

        # TODO:  look into dask for holding chunks of data for import
        # Create the file path to the csv data
        input_df = pd.read_csv(csvfile)

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

        # This is not working unfortunately:
        #help(RxDataSource)
        #data = RxDataSource(open(os.path.join(file_path, 'Campaign_Detail.csv'), 'r'))

        # The method rx_import_datasource expects a pandas df or an 
        #   RxDataSource

        rx_import_datasource(inData=input_df, \
            outFile=outfile)

        # Right now can only run once because overwrite is not working
        # rx_import_datasource(inData=campaign_detail_df, \
        #     outFile=Campaign_Detail, overwrite=True)

        # NB: overwrite param not accepting bool values so this can only be run once righ now!

    #####################################################################
    # Set up a query on table for train and test data (and ensure factor levels)
    #####################################################################

    # Train data
    data_source_train = RxSqlServerData(
        sqlQuery = "SELECT TOP 10000 * FROM Lead_Demography_Tbl ORDER BY Lead_Id", 
        connectionString = CONNECTION_STRING,
        colInfo = { # NB: may want to add all cols here
            "No_Of_Children" : { "type" : "integer" },
            "Household_Size" : { "type" : "integer" },
            "No_Of_Dependents" : { "type" : "integer" },
            "Highest_Education" : { 
                "type" : "factor",
            #     "levels" : [
            #         "High School",
            #         "College",
            #         "Attended Vocational",
            #         "Graduate School"
            #         ]
                },
            "Annual_Income_Bucket" : { 
                "type" : "factor",
                # "levels" : [ 
                #     "60k-120k",
                #     ">120k",
                #     "<60k"
                #     ]
                }
            }
        )

    # Import training data RxImport style from new query source       
    X_y_train = rx_import_datasource(data_source_train)
    print(X_y_train)

    # Test data (let's pick ~30% size of training dataset)
    data_source_test = RxSqlServerData(
        sqlQuery = "SELECT * FROM Lead_Demography_Tbl ORDER BY Lead_Id OFFSET 10000 ROWS FETCH FIRST 3000 ROW ONLY", 
        connectionString = CONNECTION_STRING,
        colInfo = { # NB: may want to add all cols here
            "No_Of_Children" : { "type" : "integer" },
            "Household_Size" : { "type" : "integer" },
            "No_Of_Dependents" : { "type" : "integer" },
            "Highest_Education" : { 
                "type" : "factor",
                # "levels" : [
                #     "",
                #     "High School",
                #     "College",
                #     "Attended Vocational",
                #     "Graduate School"
                #     ]
                },
            "Annual_Income_Bucket" : { 
                "type" : "factor",
                # "levels" : [ 
                #     "60k-120k",
                #     ">120k",
                #     "<60k"
                #     ]
                }
            }
        )

    # Import data RxImport style from new query source       
    X_y_test = rx_import_datasource(data_source_test)

    #####################################################################
    # Run revoscalepy linear regression and summary (in-database)
    #####################################################################

    mod = rx_lin_mod_ex(formula="No_Of_Children ~ F(Annual_Income_Bucket)+F(Highest_Education)", 
        data=X_y_train, compute_context=compute_context)
    assert mod is not None
    assert mod._results is not None
    pprint(mod._results)
    summary = rx_summary(formula="No_Of_Children ~ F(Annual_Income_Bucket)+F(Highest_Education)", 
        data=X_y_train, compute_context=compute_context)
    # print(summary) # NB: Doesn't want to print
    
    # pprint(vars(mod))

    #####################################################################
    # Run scikit-learn linear regression (in-memory)
    #####################################################################

    df_train = pd.DataFrame(X_y_train)
    df_test = pd.DataFrame(X_y_test)

if __name__ == '__main__':

    file_path = os.path.join(BASE_DIR, 'Data')
    inputfile = os.path.join(file_path, "Lead_Demography.csv")

    main(tablename="Lead_Demography_Tbl", csvfile=inputfile, overwrite=False)