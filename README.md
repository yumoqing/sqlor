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

## Support Database Types
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

```
import asyncio

from sqlor.dbpools  import DBPools, sqlorContext

dbs={
        "mydb":{
                "driver":"mysql",
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

