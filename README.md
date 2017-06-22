# Getting Started

## Setup

### Python Services

You'll need SQL Server 2017 first.  Get the Community Technology Preview (CTP) using your MSDN Subscription or by downloading from Microsoft [here](https://www.microsoft.com/en-us/sql-server/sql-server-2017).

Follow the installation instructions at [https://docs.microsoft.com/en-us/sql/advanced-analytics/python/setup-python-machine-learning-services](https://docs.microsoft.com/en-us/sql/advanced-analytics/python/setup-python-machine-learning-services).  It is recommended to select **Developer** as the free edition during installation and call the instance something informative like SQLSERVER2017 in case you have other versions of SQL Server on your system.  Also, right before the actual installation take note of the location of the config file that looks like:

    C:\Program Files\Microsoft SQL Server\140\Setup Bootstrap\Log\20170619_161507\ConfigurationFile.ini

The **Python Services** should be installed into a folder that looks like:

    C:\Program Files\Microsoft SQL Server\MSSQL14.SQLSERVER2017\PYTHON_SERVICES

### Programming Environment

It's recommended to use **Visual Studio 2015 or 2017** (Community Edition is fine).

New project.

Open repo folder.

Right click on **Python Environments**.  Click **Add/Remove Python Environments...** and follow the instructions [here](https://docs.microsoft.com/en-us/visualstudio/python/python-environments#creating-an-environment-for-an-existing-interpreter).  (This process may take some time)

## Test Setup

## Next Steps
