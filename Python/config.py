##########################################################################################################################################
## This Python config file sets the compute context used in all the rest of the scripts.
## The connection string below is pre-poplulated with the default values created for a VM 
## from the Cortana Intelligence Gallery.
## Change the values accordingly for your implementation.
##
## NOTE: The database named in this string must exist on your server. 
##       If you will be using the Python IDE scripts from scratch,  first go to SSMS 
##       and create a New Database with the name you wish to use.
##########################################################################################################################################
from revoscalepy.computecontext.RxLocalSeq import RxLocalSeq
import os

# Use the environment variable SQLSERVER_NAME or fill in your info here
SQLSERVER_NAME = os.getenv('SQLSERVER_NAME', '<computername>\<sql server instance name>')

CONNECTION_STRING = "Driver=SQL Server;Server=" + SQLSERVER_NAME + \
    ";Database=Campaign;Trusted_Connection=Yes"
BASE_DIR = "c:\\users\\michhar\\documents\\visual studio 2015\\Projects\\python-server-campaign-optimization\\python-server-campaign-optimization"
LOCAL = RxLocalSeq()