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

* python 3.5 or above
* asyncio
* Oracle DBAPI2 driver(cx_Oracle)
* MySQL DBAPI2 driver(mysql-connector)
* Postgresql DBAPI2 driver(psycopg2-binrary)
* Asynchronous MySQL driver(aiomysql)
* Asynchronous Postgresql driver(aiopg)
* Other driver can be easy integreated

## Using

```
import asyncio

from sqlor.dbpools  import DBPools

dbs={
        "aiocfae":{
                "driver":"aiomysql",
                "async_mode":True,
                "coding":"utf8",
                "dbname":"cfae",
                "kwargs":{
                        "user":"test",
                        "db":"cfae",
                        "password":"test123",
                        "host":"localhost"
                }
        },
		"stock":{
			"driver":"aiopg",
			"async_mode":True,
			"codeing":"utf-8",
			"dbname":"stock",
			"kwargs":{
				"dbname":"stock",
				"user":"test",
				"password":"test123",
				"host":"127.0.0.1"
			}
		},
        "cfae":{
                "driver":"mysql.connector",
                "coding":"utf8",
                "dbname":"cfae",
                "kwargs":{
                        "user":"test",
                        "db":"cfae",
                        "password":"test123",
                        "host":"localhost"
                }
        }
}

loop = asyncio.get_event_loop()
pool = DBPools(dbs,loop=loop)

async def testfunc():
        @pool.runSQL
        def sql(db,ns,callback):
                return {
                "sql_string":"select * from product",
        }
        x  = await sql('cfae',{},print)
        x  = await sql('aiocfae',{},print)

loop.run_until_complete(testfunc())
```

## API


### Databases description data(dbdesc)

sqlor uses a dbdesc data(databases description data) which description 
how many databases and what database will using, and them connection parameters to create a dbpools objects

dbdesc data is a dict data, format of the dbdesc as follow:
```
{
        "aiocfae":{			# name to identify a database connect
                "driver":"aiomysql",	# database dbapi2 driver package name 
                "async_mode":True,	# indicte this connection is asynchronous mode
                "coding":"utf8",	# charset coding
                "dbname":"cfae",	# database real name
                "kwargs":{		# connection parameters
                        "user":"test",
                        "db":"cfae",
                        "password":"test123",
                        "host":"localhost"
                }
        },
        "cfae":{
                "driver":"mysql.connector",
                "coding":"utf8",
                "dbname":"cfae",
                "kwargs":{
                        "user":"test",
                        "db":"cfae",
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

