import os
import sys
import asyncio

from appPublic.worker import get_event_loop
from appPublic.jsonConfig import getConfig
from appPublic.worker import AsyncWorker
from sqlor.dbpools import DBPools
from appPublic.dictObject import DictObject
from xls2ddl.xlsxData import XLSXData
def chunked(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]
	
async def load_tabledata(dbname, tblname, data):
	db = DBPools()
	async with db.sqlorContext(dbname) as sor:
		for r in data:
			try:
				await sor.D(tblname, {'id': r.id})
			except:
				pass
			await sor.C(tblname, r.copy())
		
async def load_data():
	if len(sys.argv) < 4:
		print(f'{sys.argv[0]} server_path dbname datafile')
		return 1
	runpath = sys.argv[1]
	dbname = sys.argv[2]
	datafile = sys.argv[3]
	config = getConfig(runpath)
	db = DBPools(config.databases)
	xlsx = XLSXData(datafile)
	worker = AsyncWorker(maxtask=100)
	tasks = []
	for i,s in enumerate(xlsx.book.worksheets):
		tblname = xlsx.book.sheetnames[i]
		dic = xlsx.readRecords(tblname, s)
		for chunk in chunked(dic[tblname], 100):
			t = asyncio.create_task(load_tabledata(dbname, tblname, chunk))
			tasks.append(t)
	await asyncio.wait(tasks)
	return 0

def main():
	get_event_loop().run_until_complete(load_data())

if __name__ == '__main__':
	main()
