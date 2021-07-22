#!/usr/bin/python3
import sys
import codecs
from sqlor.dbpools import runSQL
import asyncio

def appinit():
	if len(sys.argv) < 4:
		print(f'usage:\n {sys.argv[0]} path dbname sqlfile [k=v ...] \n')
		sys.exit(1)
	p = ProgramPath()
	if len(sys.argv) > 1:
		p = sys.argv[1]
	config = getConfig(p)
	DBPools(config.databases)

	
async def run(ns):
	with codecs.open(sys.argv[3], 'r', 'utf-8') as f:
		sql = f.read()
		await runSQL(sys.argv[2], sql, ns)

if __name__ == '__main__':
	ns = {}
	for x in sys.argv[3:]:
		try:
			k,v = x.split('=')
			ns.update({k:v})
		except Exception as e:
			print(x, 'key-value pair expected')
			print(e)
		
	appinit()
	loop = asyncio.get_event_loop()
	loop.run_until_complete(run(ns))
