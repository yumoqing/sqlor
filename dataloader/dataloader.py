import openpyxl as xlsx
import asyncio
from sqlor.dbpools import DBPools

class CBObject:
	def __init__(self,db, name):
		self.db = db
		self.tbl = name

	async def handle(self,ws):
		db = DBPools()
		meta = sor.I()
		async with db.sqlorContext(self.db) as sor:
			for rec in getRecord(ws):
				sor.C(self.tbl, rec)

typesconv = {
	"int":int,
	"float":float,
	"str":str,
}

async def getRecord(ws):
	names = []
	types = []
	for i,r in enumerate(ws.rows):
		if i==0:
			for j,c in enumerate(r):
				nt = c.value.split(':')
				if len(nt) < 2:
					nt.append('')
				n,t = nt
				names.append(n)
				types.append(t)
		else:
			dic = {}
			for j,c in enumerate(r):
				tf = typesconv.get(types[j],None)
				v = c.value
				dic[names[j]] = v
			yield rec

async def excel2db(xlsxfile):
	wb = xlsx.load_workbook(xlsxfile)
	dbname = [ i[2:-3] for i in wb.sheetnames if i.startswith('__')[0]
	for name in wb.sheetnames:
		if name.startswith('__'):
			continue
		ws = wb[name]
		cbobj = CBObject(dbname, name)
		await cbobj.handle(ws)

if __name__ == '__main__':
	import sys
	config = getConfig()
	DBPools(config.databases)
	if len(sys.argv) < 2:
		print('%s xlsxfile' % sys.argv[0])
		sys.exit(1)
	loop = asyncio.get_event_loop()
	loop.run_until_complete(excel2db(sys.argv[1]))
