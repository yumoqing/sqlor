import openpyxl as xlsx
import asyncio

class CBObject:
	def __init__(self,name):
		self.tbl = name
		self.sql = ''

	async def handle(self,dic):
		names = ','.join(dic.keys())
		vs = ["'%s'"%v if type(v)==type('') else str(v) for v in dic.values() ]
		values = ','.join(vs)
		self.sql = "insert into " + self.tbl +  \
				" (" + names + ") values (" +  \
				values + ");"
		print(self.sql)



typesconv = {
	"int":int,
	"float":float,
	"str":str,
}

async def loadData(ws,callback):
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
				if tf is not None:
					v = tf(v)
				dic[names[j]] = v
			await callback(dic)

async def excel2db(xlsxfile):
	wb = xlsx.load_workbook(xlsxfile)
	for name in wb.sheetnames:
		ws = wb[name]
		cbobj = CBObject(name)
		await loadData(ws,cbobj.handle)

if __name__ == '__main__':
	import sys

	if len(sys.argv) < 2:
		print('%s xlsxfile' % sys.argv[0])
		sys.exit(1)
	loop = asyncio.get_event_loop()
	loop.run_until_complete(excel2db(sys.argv[1]))
