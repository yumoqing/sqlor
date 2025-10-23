# SQLOR

SQLOR is a database api for python3, it is base on the python's DBAPI2 

## Features

* Multiple database supported(Oracle, MySql, Postgresql, SQL Server
* Both asynchronous API & synchronous API supported
* Connection pools 
* Connection life cycle managements
* Easy using API
* Resources(connection object, cursor object) automatic recycled


## requirements

* python 3.9 or above
* asyncio
* Oracle DBAPI2 driver(cx_Oracle)
* Postgresql DBAPI2 driver(aiopg)
* Asynchronous MySQL driver(aiomysql)
* Asynchronous Postgresql driver(aiopg)
* clickhouse(clickhouse-connect)
* Other driver can be easy integreated

## Supported Database Types
* oracle
* mysql, mariadb
* TiDB
* clickHouse
* DuckDB
* PostgreSQL
* MsSQL

## Database Description json format
password in json data is encrypted by aes.

* mysql, tidb, mariadb
```
{
	"databases":{
		"mydb":{
			"driver":"mysql",
			"kwargs":{
				"user":"test",
				"db":"cfae",
				"password":"test123",
				"host":"localhost"
			}
		}
	}
}
```
* PostgreSQL
```
{
    "databases":{
        "mydb":{
            "driver":"postgresql",
            "kwargs":{
                "user":"test",
                "dbname":"cfae",
                "password":"test123",
                "host":"localhost"
            }
        }
    }
}
```
* duckdb
```
{
    "databases":{
        "mydb":{
            "driver":"duckdb",
            "kwargs":{
                "dbfile":"ttt.ddb"
            }
        }
    }
}
```

## Using

### sqlor setup
First, Specified a server_path folder, under the server_path folder, need a named by "conf" subfolder.
and a config.json file must be in the conf.

the config.json need "password_key" and "databases" attributes, like:
```
{
	.......
	"password_key":"tfyugihjo245g7g642yubv24g534",
	"databases":{
		.......
        "mydb":{
                "driver":"mysql",
                "kwargs":{
                        "user":"test",
                        "db":"database_name_in_your_database_engine",
                        "password":"encoded_password_string",
                        "host":"localhost"
                }
        }
	}
}
```
### generates encoded password string
sqlor has a dbpassword script let you generate the "encoded_password_string" in config.json
dbpassword usage:
```
dbpassword server_path database_password_of_user
```

### script to use sqlor

```
import asyncio
from appPublic.worker import get_event_loop
from appPublic.jsonConfig import getConfig
from sqlor.dbpools  import DBPools, sqlorContext

loop = get_event_loop()
config = getConfig(server_path)
pool = DBPools(config.databases, loop=loop)

async def testfunc():
	dbname = 'mydb'
	db = DBPools()
	async with sqlorContext('stock') as sor:	
		# start a transaction
		# if exception happended, all change to database will rollback
		# else will commit
		sql = "select * from stock where stock_num = ${stock_num}"
		recs = await sor.sqlExe(sql, {'stock_num':'688888'})
		# return a list of DictObject instance
		sql1 = "select * from stock"
		recs = await sor.sqlPaging(sql, {'pagerows':50, 'page':1, 'sort':'stock_id'})
		# return a dictionary {
		#  'total': return all reocords count.
		#  'rows': list of DictObject instance
		# }
		ns = {'id':'666667'} # filters dictionary, find all records which id = '666667'
		recs = await sor.R('stock', ns)
		# recs is a list of data in DictObject instance
		ns = {'pagerows': 50, 'page':1, 'sort':stock_id'}
		# find all record in table 'stock', return page 1 data which 50 max records 
		dic = await sor.R('stock', ns)
		# return a dictionary {
		#  'total': return all reocords count.
		#  'rows': list of DictObject instance
		# }
		await sor.C('stock', {
			'stock_id': '111111',
			...
		})
		# add a record to 'stock' table
		await sor.D('stock', {'stock_id':'111111'})
		# delete a record in 'stock' table which stock_id = '111111'
		await sor.U('stock', {'stock_id':'111111', 'name':'new_name'})
		# update name field's value to 'new_name' which stock_id = '111111'


loop.run_until_complete(testfunc())
```

## scripts
### dbpassword
generate a encoded password for the password of the user of database

* Syntax

dbpassword server_path password_text

1 server_path is folder where server_path/conf/config.json file have specifies password_key and databases

2 password_text is the password of the specified user of database

* description


dbpassword encodes second argument password to a base64 based cyber, this cyber need to write into the password attribute under kwargs, and print the cyber to stdout

### dbloader
load data in xlsx file to your database

* Syntax
dbloader server_path dbname xlsxfile

1 server_path is folder where server_path/conf/config.json file have specifies password_key and databases

2 dbname is database where the data will insert into

3 xlsxfile is a data file, can contains many sheets, sheet name is tablename, first rows of sheet contains the field name of the table, fields without data not need to appear in the sheet.


* dbloader get data from each named sheet in xlsxfile, and insert them to database parellally, 

## API


### Databases description data(dbdesc)

sqlor uses a dbdesc data(databases description data) which description 
how many databases and what database will using, and them connection parameters to create a dbpools objects

dbdesc data is a dict data, format of the dbdesc as follow:
```
{
        "mydb1":{			# name to identify a database connect
                "driver":"mysql",	# database dbapi2 driver package name 
                "async_mode":True,	# indicte this connection is asynchronous mode
                "coding":"utf8",	# charset coding
                "dbname":"cfae",	# database real name
                "kwargs":{		# connection parameters
                        "user":"test",
                        "db":"cfae",
                        "password":"encoded_password",
                        "host":"localhost"
                }
        },
        "mydb2":{
                "driver":"postgresql",
                "kwargs":{
                        "user":"test",
                        "dbname":"cfae",
                        "password":"test123",
                        "host":"localhost"
                }
        }
}

```
sqlor can using multiple databases and difference databases by using difference database driver

### sql description data


## class

### DBPools

### SQLor

