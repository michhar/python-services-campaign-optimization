"""

Logistic regression analysis on demographic data

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

from revoscalepy import RxComputeContext
from revoscalepy import RxSqlServerData
from revoscalepy import RxInSqlServer
from revoscalepy import RxOdbcData
from revoscalepy import rx_import
from revoscalepy import RxXdfData
from revoscalepy import RxTextData
from revoscalepy import rx_data_step
from revoscalepy import rx_get_info
from revoscalepy import rx_lin_mod
from revoscalepy import rx_logit
from revoscalepy import rx_predict
from revoscalepy import rx_summary
from revoscalepy import rx_set_compute_context
from sklearn.metrics import accuracy_score
from sklearn.preprocessing import binarize
from sklearn.linear_model import LinearRegression
from config import CONNECTION_STRING, BASE_DIR
import os
import pandas as pd
import numpy as np
from pprint import pprint


def main(tablename, inputdf, overwrite=False):
    """Imports a data source into SQL Server table and, operating
       in-database, uses logistic regression to create a
       predictive model.

       A comparison to an out-of-database, in memory method
       is performed (TODO)
       
       Parameters
       ----------

       tablename : str
           The new or previosly create table name in database.

       inputdf : RxTextData object
           The data source

       overwrite : bool, optional (default=False)
           Whether or not to overwrite the table.  
           Set to True if this is a new table, otherwise False.
       """

    ####################################################################
    # Set the compute context to SQL SERVER
    ####################################################################

    # NB: don't need, but would be good to know what this actually does here
    # RxComputeContext(LOCAL, '9.1')

    compute_context = RxInSqlServer(connection_string = CONNECTION_STRING)
        # ,
        # num_tasks = 1,
        # auto_cleanup = False,
        # console_output=True
        # )
    
    rx_set_compute_context(compute_context)

    # if overwrite:

    ####################################################################
    # Create table in SQL server
    ####################################################################

    print("Creating tables...")
    data_source = RxSqlServerData(
        table=tablename, 
        connection_string=CONNECTION_STRING)

    ####################################################################
    # Read data into the SQL server table that was just created
    ####################################################################

    print("Reading data into tables...")

    rx_data_step(input_data=inputdf, output_file=data_source, overwrite=True)

    #####################################################################
    # Set up a query on table for train and test data (and ensure factor levels)
    #####################################################################
    print("Setting up query and datasources for train and test sets...")

    # Train data
    data_source_train = RxSqlServerData(
        sql_query="SELECT TOP 10000 * FROM Lead_Demography_Tbl \
                    ORDER BY Lead_Id", 
        connection_string=CONNECTION_STRING,
        verbose=True
        )

    # Import training data RxImport style from new query source       
    X_y_train = rx_import(data_source_train)
    # X_y_train = rx_data_step(input_data=data_source_train, overwrite=True)


    print("Test data...")
    # Test data (let's pick ~30% size of training dataset)
    data_source_test = RxSqlServerData(
        sql_query="SELECT * FROM Lead_Demography_Tbl \
                    ORDER BY Lead_Id \
                    OFFSET 10000 ROWS \
                    FETCH FIRST 3000 ROW ONLY", 
        connection_string=CONNECTION_STRING
        )

    # Import data RxImport style from new query source       
    X_y_test = rx_import(data_source_test)
    # X_y_test = rx_data_step(input_data=data_source_test, overwrite=True)

    #####################################################################
    # Run revoscalepy logistic regression on training data (in-database)
    #####################################################################

    print('Fitting a logistic regression model...')

    mod = rx_logit(formula="Annual_Income_Bucket_gt120k ~ \
                                F(Highest_Education_Graduate_School)", 
        data=X_y_train, compute_context=compute_context, verbose=2)
    assert mod is not None
    assert mod._results is not None
    pprint(mod._results)

    #####################################################################
    # Summary on training data (in-database)
    #####################################################################

    # Note:  for "data" use data source and not the rximport'ed data

    print('\nSummary: \n')

    summary = rx_summary("Annual_Income_Bucket_gt120k ~ \
                                F(Highest_Education_Graduate_School)",
        data=data_source_train, compute_context=compute_context,
        cube=True, verbose=2)

    #####################################################################
    # Predict on test data (in-database)
    #####################################################################

    print("\nPredict on test: \n")

    pred = rx_predict(mod, data=X_y_test, verbose=2, write_model_vars=True)

    #####################################################################
    # Metrics for predition based on groundtruth (with scikit-learn tools)
    #####################################################################  

    pred_results = pred._results['Annual_Income_Bucket_gt120k_Pred']
    # For some reason the prediction results are not in a binary [0,1] format
    y_pred = binarize(pred_results, threshold=(min(pred_results) + \
                        max(pred_results))/2).reshape(-1, 1)
    y_true = pred._data['Annual_Income_Bucket_gt120k']

    
    print("Model prediction results:", y_pred)
    print("Actual values: ", y_true)

    print("Accuracy score: ", accuracy_score(
                        y_true=y_true, 
                        y_pred=y_pred))
    
    
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

    ####################################################################
    # Dummy encode variables of interest with an xdf file as output
    ####################################################################

    def dummy_vars(df, context):
        # Dummy vars
        df_dummy = pd.get_dummies(df, 
            columns=['Annual_Income_Bucket', 'Highest_Education'])

        # Rename columns (remove spaces etc.)
        new_cols = [x.replace(' ', '_').replace('>', 'gt').replace('<', 'lt') 
                        for x in df_dummy.columns]
        df_dummy.columns = new_cols
        return df_dummy

    output_file = os.path.join(file_path, "Lead_Demography.xdf")

    # Set a csv as a data source
    data_source = RxTextData(file=inputfile, delimiter=',')

    # Dummy variables and output to named xdf
    rx_data_step(input_data=data_source, output_file=output_file,
                                overwrite=True, transform_function=dummy_vars)

    rx_get_info(data=output_file, verbose=2)

    ####################################################################
    # Call main function to work in SQL compute context
    ####################################################################

    main(tablename="Lead_Demography_Tbl", inputdf=output_file, 
            overwrite=True)