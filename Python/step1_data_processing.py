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
from revoscalepy.functions.RxPredict import rx_predict_ex
from revoscalepy.functions.RxSummary import rx_summary
from revoscalepy.utils.RxOptions import RxOptions

from config import CONNECTION_STRING, BASE_DIR, LOCAL
import os
import pandas as pd

# The following is simple proof of concept and will be 
#   refactored into functions for release

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

####################################################################
# Read in data into a pandas df from a file
####################################################################

# TODO:  look into dask for holding chunks of data for import
# Create the file path to the csv data
file_path = os.path.join(BASE_DIR, 'Data')
campaign_detail_df = pd.read_csv(os.path.join(file_path, 'Campaign_Detail.csv'))

####################################################################
# Create table in SQL server
####################################################################

print("Creating tables...")
Campaign_Detail = RxSqlServerData(table = "Campaign_Detail", connectionString = CONNECTION_STRING)

####################################################################
# Read data into the SQL server table just created
####################################################################

print("Reading data into tables...")

# This is not working unfortunately:
#help(RxDataSource)
#data = RxDataSource(open(os.path.join(file_path, 'Campaign_Detail.csv'), 'r'))

# The method rx_import_datasource expects a pandas df or an 
#   RxDataSource

# Right now can only run once because overwrite is not working
# rx_import_datasource(inData=campaign_detail_df, \
#     outFile=Campaign_Detail, overwrite=True)

# NB: overwrite param not accepting bool values so this can only be run once righ now!

#####################################################################
# Run a query on table
#####################################################################

data_source = RxSqlServerData(
    sqlQuery = "SELECT * FROM Campaign_Detail", 
    connectionString = CONNECTION_STRING,
    colInfo = { # NB: may want to add all cols here
        "Call_For_Action" : { "type" : "integer" }, 
        "Tenure_Of_Campaign" : { "type" : "integer" }
    })

# Import data RxImport style from new query source to avoid factor levels       
data = rx_import_datasource(data_source)
print(data)

#####################################################################
# Run linmod
#####################################################################

# NB:  not working due to ""'sp_execute_external_script' is disabled on this instance of SQL Server"
#   error even though it has been set to 1 in SSMS

linmod = rx_lin_mod_ex("Call_For_Action ~ Tenure_Of_Campaign", data = data, compute_context = compute_context)
assert linmod is not None
assert linmod._results is not None
print(linmod)