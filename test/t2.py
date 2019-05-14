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

async def testfunc():
	@pool.runSQL
	def sql(db,ns,callback):
		return {
		"sql_string":"select * from product",
	}
	x  = await sql('cfae',{},print)

loop.run_until_complete(testfunc())
