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
async def printTables(dbname):
	r = await pool.getTables(dbname)
	print('tables=',r)

async def printFields(dbname,tablename):
	r = await pool.getTableFields(dbname,tablename)
	print(dbname,tablename,'fields=',r)

async def printPrimary(dbname,tablename):
	r = await pool.getTablePrimaryKey(dbname,tablename)
	print(dbname,tablename,'primary key=',r)

loop.run_until_complete(printTables('cfae'))
loop.run_until_complete(printFields('cfae','product'))
loop.run_until_complete(printPrimary('cfae','product'))
