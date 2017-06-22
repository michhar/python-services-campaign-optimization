"""

Campaign optimization step 1

* Import data to SQL Server
* Process in-database

Micheleen Harris
"""

from revoscalepy.computecontext.RxComputeContext import RxComputeContext
from revoscalepy.computecontext.RxInSqlServer import RxInSqlServer
from revoscalepy.etl.RxImport import rx_import_datasource

from config import CONNECTION_STRING, BASE_DIR
import os
import pandas as pd

computeContext = RxInSqlServer(
    connectionString = CONNECTION_STRING,
    numTasks = 1,
    autoCleanup = False
    )

file_path = BASE_DIR + os.path.sep + 'Data'

# TODO:  look into dask for holding chunks of data for import

campaign_detail_df = pd.read_csv(os.path.join(file_path, 'Campaign_Detail.csv'))


#data = rx_import_datasource(dataSource)