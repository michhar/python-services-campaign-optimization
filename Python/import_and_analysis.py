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

Based on: https://docs.microsoft.com/en-us/sql/advanced-analytics/tutorials/use-python-revoscalepy-to-create-model

Micheleen Harris
"""

from revoscalepy.computecontext.RxComputeContext import  RxComputeContext
from revoscalepy.computecontext.RxInSqlServer import RxSqlServerData
from revoscalepy.computecontext.RxInSqlServer import RxInSqlServer
from revoscalepy.computecontext.RxInSqlServer import RxOdbcData
from revoscalepy.etl.RxImport import rx_import_datasource
from revoscalepy.datasource.RxXdfData import RxXdfData
from revoscalepy.datasource.RxFileData import RxFileData
from revoscalepy.functions.RxDataStep import rx_data_step_ex
from revoscalepy.functions.RxLinMod import rx_lin_mod_ex
from revoscalepy.functions.RxLogit import rx_logit_ex
from revoscalepy.functions.RxPredict import rx_predict_ex
from revoscalepy.functions.RxSummary import rx_summary
from sklearn.metrics import mean_squared_error, r2_score, accuracy_score
from sklearn.preprocessing import binarize
from sklearn.linear_model import LinearRegression

from config import CONNECTION_STRING, BASE_DIR
import os
import pandas as pd
import numpy as np
import dask.dataframe as dd
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

       overwrite : bool, optional (default=False)
           Whether or not to overwrite the table.  
           Set to True if this is a new table, otherwise False.
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
        colInfo = {
            "No_Of_Children" : { "type" : "integer" },
            }
        )

    # Import training data RxImport style from new query source       
    X_y_train = rx_import_datasource(data_source_train)

    # Test data (let's pick ~30% size of training dataset)
    data_source_test = RxSqlServerData(
        sqlQuery = "SELECT * FROM Lead_Demography_Tbl \
                    ORDER BY Lead_Id \
                    OFFSET 10000 ROWS \
                    FETCH FIRST 3000 ROW ONLY", 
        connectionString = CONNECTION_STRING,
        colInfo = {
            "No_Of_Children" : { "type" : "integer" },
            "Annual_Income_Bucket_gt120k" : {"typte" : "factor"},
            "Highest_Education_Graduate_School" : {"typte" : "factor"}
            }
        )

    # Import data RxImport style from new query source       
    X_y_test = rx_import_datasource(data_source_test)

    #####################################################################
    # Run revoscalepy linear regression on training data (in-database)
    #####################################################################

    mod = rx_logit_ex(formula="Annual_Income_Bucket_gt120k ~ \
                                F(Highest_Education_Graduate_School)", 
        data=X_y_train, compute_context=compute_context, verbose=2)
        # cov_coef=True)
    assert mod is not None
    assert mod._results is not None
    pprint(mod._results)

    #####################################################################
    # Summary on training data (in-database)
    #####################################################################

    # Note:  for "data" use data source and not the rximport'ed data

    print('\nSummary: \n')

    summary = rx_summary("F(Annual_Income_Bucket_gt120k) ~ \
                                F(Highest_Education_Graduate_School)", 
        data=data_source_train, compute_context=compute_context,
        cube=True, verbose=2)

    #####################################################################
    # Predict on test data (in-database)
    #####################################################################

    print("\nPredict on test: \n")

    pred = rx_predict_ex(mod, data=X_y_test, verbose=2, write_model_vars=True)

    #####################################################################
    # Metrics for predition based on groundtruth (with scikit-learn tools)
    #####################################################################  

    pred_results = pred._results['Annual_Income_Bucket_gt120k_Pred']
    # For some reason the prediction results are not in a binary [0,1] format
    y_pred = binarize(pred_results, threshold=(min(pred_results) + max(pred_results))/2).reshape(-1, 1)
    y_true = pred._data['Annual_Income_Bucket_gt120k']

    
    print("Model prediction results:", y_pred)
    print("Actual values: ", y_true)

    print("Accuracy score: ", accuracy_score(
                        y_true=y_true, 
                        y_pred=y_pred))



    # # NB: If this is regression (rx_lin_mod_ex) can look at metrics like:
    # print("R^2 (coefficient of determination): ", r2_score(
    #                     y_true=y_true, 
    #                     y_pred=y_pred))
    
    
    #####################################################################
    # Run scikit-learn linear regression (in-memory) - TODO
    #####################################################################

    # df_train = pd.DataFrame(X_y_train)
    # df_test = pd.DataFrame(X_y_test)

if __name__ == '__main__':

    ####################################################################
    # Read in data into a pandas df from a file (do here for manipulation)
    ####################################################################

    file_path = os.path.join(BASE_DIR, 'Data')
    # Note: converted the 'Lead_Id' column to integer by removing chars
    inputfile = os.path.join(file_path, "Lead_Demography.csv")

    # Create the file path to the csv data (could use dask to do some 
    #   preprocessing out-of-core - see "Dask way")
    input_df = pd.read_csv(inputfile)

    ####################################################################
    # Dummy encode variables of interest with pandas
    ####################################################################

    df_dummy = pd.get_dummies(input_df, 
        columns=['Annual_Income_Bucket', 'Highest_Education'])

    # Fix column names
    new_cols = [x.replace(' ', '_').replace('>', 'gt').replace('<', 'lt') 
        for x in df_dummy.columns]
    print(new_cols)
    df_dummy.columns = new_cols

    ####################################################################
    # Dask way - optional perf speedup and work in progress
    ####################################################################

    # # Read a csv file into a dask dataframe (data chunked on disk) 
    # #   - must have numeric id, and dummy variables

    # input_df = dd.read_csv(inputfile, dtype={'Annual_Income_Bucket' : 'category',
    #     'Highest_Education' : 'category'})

    # df_dummy = dd.get_dummies(input_df.categorize(), 
    # columns=['Annual_Income_Bucket', 'Highest_Education']).persist()

    # dummyfile = os.path.join(file_path, "Lead_Demography_dummied.csv")

    # df_dummy.to_csv(dummyfile)
    # print(dummyfile)


    ####################################################################
    # Import from file directly to xdf - work in progress
    ####################################################################

    # def dummy_vars(df):
    #     df_dummy = pd.get_dummies(df, 
    #         columns=['Annual_Income_Bucket', 'Highest_Education'])
    #     return df_dummy

    # def test(data):
    #     print(type(data))
    #     return data

    # xdf = RxFileData(file=inputfile, class_name='DataFrame', return_data_frame=True)

    # rx_data_step_ex(input_data=xdf, output_file=xdf, 
    #                 transform_function=test,
    #                 overwrite=True)

    # tf = rx_import_datasource(xdf)
    # # colInfo = { # NB: may want to add all cols here
    # #         "No_Of_Children" : { "type" : "integer" },
    # #         "Household_Size" : { "type" : "integer" },
    # #         "No_Of_Dependents" : { "type" : "integer" }
    # #         }
    # #     )

    # # Wish to implement these revoscaler functions for revoscalepy:
    # # rxGetInfo(tf, getVarInfo = TRUE, numRows = 10)
    # # rxDataStep(tf, tf, transformFunc = makeDummies, overwrite = TRUE)
    # # rxGetInfo(tf, getVarInfo = TRUE, numRows = 10)


    ####################################################################
    # Call main function to work in SQL compute context
    ####################################################################

    main(tablename="Lead_Demography_Tbl", inputdf=input_df, 
        overwrite=False)