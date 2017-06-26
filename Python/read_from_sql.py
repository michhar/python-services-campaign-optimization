"""
Purpose:  demonstrate ways to read from a SQL table and 
    keep perform computation on disk.
"""

from revoscalepy.computecontext.RxInSqlServer import RxSqlServerData
from revoscalepy.computecontext.RxInSqlServer import RxInSqlServer
from revoscalepy.etl.RxImport import rx_import_datasource
from revoscalepy.utils.RxUtils import rx_print_header
import dask.dataframe as dd
from config import CONNECTION_STRING
from urllib.parse import quote_plus
import os


####
# Read a SQL table into a dask dataframe (data chunked on disk)
####

print("Reading from SQL table into dask df...")

SQLALCHEMY_DATABASE_URI = "mssql+pyodbc:///?odbc_connect=%s" % quote_plus(CONNECTION_STRING)
df = dd.read_sql_table('Lead_Demography_Tbl_WithID', SQLALCHEMY_DATABASE_URI,
    index_col='Lead_Id')

print(df.columns)

####
# Write to a file (to_csv)
####

###
# Read a SQL table (in-place in the DB!) with revoscalepy
###

print("Reading from SQL table in-place with revoscalepy...")

compute_context = RxInSqlServer(
    connectionString = CONNECTION_STRING,
    numTasks = 1,
    autoCleanup = False
)

data_source = RxSqlServerData(
    sqlQuery = "SELECT * FROM Lead_Demography_Tbl_WithID \
                ORDER BY Lead_Id", 
    connectionString = CONNECTION_STRING,
    colInfo = { # NB: may want to add all cols here
        "No_Of_Children" : { "type" : "integer" },
        "Household_Size" : { "type" : "integer" },
        "No_Of_Dependents" : { "type" : "integer" }
        }
    )

# Import training data RxImport style from new query source
# NB: data is still living in the SQL table!      
X = rx_import_datasource(data_source)
print(rx_print_header(data_source=X))


###
# Next steps
#   * Read into a table with revoscalepy
#   * Predictive analytics
#   * Viz
###

'''
NAME
    revoscalepy.functions

PACKAGE CONTENTS
    RxDTree
    RxDataStep - coming soon
    RxLinMod
    RxLogit
    RxPredict
    RxRoc - coming soon
    RxSummary
'''


