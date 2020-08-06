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

async def paging():
	@pool.runSQLPaging
	def sql(db,ns):
		return {
		"sql_string":"select * from product",
	}
	x  = await sql('aiocfae',{'rows':5,'page':1,"sort":"productid"})
	print('x=',x['total'],len(x['rows']))


loop.run_until_complete(paging())
