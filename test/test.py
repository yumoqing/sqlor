import asyncio

from sqlor.dbpools  import DBPools
from sqlor.records import Records

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

async def testfunc1():
	@pool.runSQL
	def sql(db,ns,callback):
		return {
		"sql_string":"select * from product",
	}
	recs = Records()
	x  = await sql('cfae',{},recs.add)
	print('--------------%s---------------' % x)
	for r in recs:
		print(type(r),r)

async def testfunc2():
	@pool.runSQLResultFields
	def sql(db,ns,callback=None):
		return {
		"sql_string":"select * from product",
	}
	recs = Records()
	x  = await sql('cfae',{},callback=recs.add)
	print('-------%s------' % x)
	for i in recs._records:
		print("--",i)

loop.run_until_complete(testfunc1())
loop.run_until_complete(testfunc2())
