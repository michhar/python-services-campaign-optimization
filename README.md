# Getting Started

## Introduction

This example solution uses marketing campaign data to perform in-database analyses.  The server setup is SQL Server 2017 with Python Services (see Setup).  The `Python` folder contains the scripts in Python using the `revoscalepy` package for a remote compute context.  It's a good idea to review the `revoscalepy` docs [here](https://docs.microsoft.com/en-us/sql/advanced-analytics/python/what-is-revoscalepy) for a more detailed introduction and listing of the available methods.

Folder structure:

- `Data/` - the marketing campaign data
- `Python/` - the Python scripts for data ingress and analysis in a remote compute context

## Setup

### Python Services

You'll need SQL Server 2017 first.  Get the Community Technology Preview (CTP) using your MSDN Subscription or by downloading from Microsoft [here](https://www.microsoft.com/en-us/sql-server/sql-server-2017).

Follow the installation instructions at [https://docs.microsoft.com/en-us/sql/advanced-analytics/python/setup-python-machine-learning-services](https://docs.microsoft.com/en-us/sql/advanced-analytics/python/setup-python-machine-learning-services).  It is recommended to select **Developer** as the free edition during installation and call the instance something informative like SQLSERVER2017 in case you have other versions of SQL Server on your system.  Also, right before the actual installation take note of the location of the config file that looks like:

    C:\Program Files\Microsoft SQL Server\140\Setup Bootstrap\Log\20170619_161507\ConfigurationFile.ini

The **Python Services** should be installed into a folder that looks like:

    C:\Program Files\Microsoft SQL Server\MSSQL14.SQLSERVER2017\PYTHON_SERVICES

### Programming Environment

It's recommended to use **Visual Studio Code**, but any IDE or code editor should be fine (it's always good to have some form of Python extensions in the code editor however).

New project.

Open repo folder.

> If using **Visual Studio 2015/2017** you'll need to additionally do the following:  Right click on **Python Environments**.  Click **Add/Remove Python Environments...** and follow the instructions [here](https://docs.microsoft.com/en-us/visualstudio/python/python-environments#creating-an-environment-for-an-existing-interpreter).  (This process may take some time)

### Add the Database to your instance of SQL Server 2017

You'll need to add a database for this example called `Campaign` to the SQL Server instance and the way we recommend here is with SQL Server Management Studio (SSMS) version 17.X (17.X is the version compatible with this SQL Server version).  Download it [here](https://docs.microsoft.com/en-us/sql/ssms/download-sql-server-management-studio-ssms) if you don't already have this version.

When you open up SSMS it will ask for the name of the Database Engine.  This is simply your computer's name, backslash, your instance name if you named it, e.g., `MININT1234\SQLSERVER2017`.  More detailed instructions can be found [here](https://docs.microsoft.com/en-us/sql/ssms/download-sql-server-management-studio-ssms).

After connecting to SQL Server in SSMS, we need to create a new database named `Campaign`.  Right click on **Databases** and **New Database...** and name it `Campaign`.  After adding, you should see the database appear in SSMS.

### Set the Server to be able to execute external scripts

In SSMS run the following T-SQL statement:

    sp_configure 'external scripts enabled', 1;  
    RECONFIGURE;
    
More on this [here](https://docs.microsoft.com/en-us/sql/database-engine/configure-windows/external-scripts-enabled-server-configuration-option).

## Test Setup

## Next Steps
