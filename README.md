# AppNexus currency exchage rate consumer

A python script that use the endpoint https://api.appnexus.com/currency?show_rate=true&code=CURRENCY_CODE&ymd=2012-03-01 to pull the rate per usd for defined currencies and writes the data to SQL Server. Handles user input, logs responses and important messages and validate response before delete and insert operations.
The script has also a version for adding new currencies, usefull in cases where you want to populate new currencies for large date range without affecting the existing currencies.

API Documentation: [Currency Service - Xandr API - Xandr Documentation](https://wiki.xandr.com/display/api/Currency+Service)

![4a381621.png](./img/screenshot.png?row=true)

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

## Prerequisites

`Python 3.6.5`

`SQL Server 2017 developer`

`SQL Server Managment Studio`

`Required Python packages`

## Installation
        
`Python 3`

Download the x86-64 executable installer from https://www.python.org/ftp/python/2.7.15/python-2.7.15.amd64.msi and check option for adding python on system path

Run the following command to verify that python is installed and added to path
```
py --version
```

`SQL server`: 
Follow the guide: https://www.sqlservertutorial.net/install-sql-server/

`SSMS`:
Follow the guide:
[SQL Server Management Studio (SSMS): What is, Install, Versions](https://www.guru99.com/sql-server-management-studio.html)

`Required Python packages`:
Install packages by runing the following commands on CMD
```
py -m install requests
py -m install pyodbc
```

## Configuration
`Create table`

```
USE [foo_db]

CREATE TABLE [dbo].[appnexus_exchanges](
	[date] [date] NULL,
	[name] [nchar](10) NULL,
	[rate_per_usd] [float] NULL
) 
```

`Configure table name variable`
```
table_name = '[foo_db].[dbo].[appnexus_exchanges]'
```

`Configure SQL Server connection`
```
# Replace Server with your Database Engine Server name
sql_server_connector = pyodbc.connect(
            'Driver={ODBC Driver 17 for SQL Server};'
            'Server=MAINDESKTOP\DB_PROD;'
            'Database=foo_db;'
            'Trusted_Connection=yes;'
        )
```

`Define currencies`
```
currencies = ['EUR', 'RON']
```

## Examples
Script accepts 2 arguments `-starddate` and `--enddate` in `YYYY-MM-DD` format

Run for a date range
```
py AppNexus_Exchange_Rate_pull.py --startdate 2019-10-01 --enddate 2019-12-05
```
Run for a single day
```
py AppNexus_Exchange_Rate_pull.py --startdate 2019-10-01
# or
py AppNexus_Exchange_Rate_pull.py --startdate 2019-10-01 --enddate 2019-10-01
```
Run for yesterday
```
py AppNexus_Exchange_Rate_pull.py --startdate yesterday
# or
py AppNexus_Exchange_Rate_pull.py 
```
Run for day before yesterday
```
py AppNexus_Exchange_Rate_pull.py --startdate day_before_yesterday
```
## Built With

* Python
* SQL Server

## License
This project is licensed under the MIT License - see the [LICENSE.md](https://github.com/kostiskar/currency_exchange_rate_consumer/blob/master/LICENSE) file for details
