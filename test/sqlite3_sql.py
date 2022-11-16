import sys
import os
import asyncio
import codecs
from sqlor.dbpools import DBPools

async def exesqls(sqllines):
	sqls = sqllines.split(';')
	async with DBPools().sqlorContext('db') as sor:
		for sql in sqls:
			await sor.sqlExe(sql, {}) 

async def main():
	with codecs.open(sys.argv[2],'r', 'utf-8') as f:
		txt = f.read()
		await exesqls(txt)


if __name__ == '__main__':
	if len(sys.argv) < 3:
		print(f'Usage:{sys.argv[0]} sqlite3_db_file sqlfile')
		sys.exit(1)
	dbs = {
		"db":{
			"driver":"sqlite3",
			"kwargs":{
				"dbname":sys.argv[1]
			}
		}
	}
	DBPools(dbs)
	loop = asyncio.get_event_loop()
	loop.run_until_complete(main())


