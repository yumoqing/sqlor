import aiosqlite
from .sor import SQLor

select_stmt = """
select 
	$[fields]$ 
from 
	$[tables]$ 
$[whereclause]$
$[groupbyclause]$
$[havingclause]$
$[orderbyclause]$
"""

class Aiosqliteor(SQLor):
	@classmethod
	def isMe(self,name):
		return name=='aiosqlite'

	async def connect(self):
		"""
		dbdesc:
			path: 数据库文件路径（或 :memory:）
		"""
		dbdesc = self.dbdesc
		self.dbpath = dbdesc.get('path', ':memory:')
		self.conn = await aiosqlite.connect(self.dbpath)
		self.conn.row_factory = aiosqlite.Row  # 支持 dict 访问
		self.dbname = self.dbpath

	async def close(self):
		await self.conn.close()

	async def enter(self):
		"""开启事务"""
		self.cur = await self.conn.cursor()

	async def exit(self):
		"""释放 cursor"""
		try:
			await self.cur.close()
		except:
			pass
		self.cur = None

	def grammar(self):
		return {
			'select':select_stmt,
		}
		
	def placeHolder(self,varname,i):
		if varname=='__mainsql__' :
			return ''
		return '?'

	def dataConvert(self,dataList):
		if type(dataList) == type({}):
			return dataList
		d = { i['name']:i['value'] for i in dataList }
		return d

	def pagingSQLmodel(self):
		return u"""select * from (%s) page_s limit $[page_size]$ offset $[offset]$"""

	def tablesSQL(self):
		sqlcmd = """SELECT name,sql as title FROM sqlite_master WHERE type='table'"""
		return sqlcmd
	
	def fieldsSQL(self,tablename=None):
		sqlcmd="""PRAGMA table_info(%s);""" % tablename
		return sqlcmd

	def fkSQL(self,tablename=None):
		tablename = tablename.lower()
		sqlcmd = """PRAGMA foreign_key_list('%s');""" % tablename
		return sqlcmd

	def pkSQL(self,tablename=None):
		tablename = tablename.lower()
		sqlcmd="""PRAGMA table_info('%s');""" % tablename
		return sqlcmd
		
	def indexesSQL(self,tablename=None):
		sqlcmd = """PRAGMA index_list('%s');""" % tablename
		return sqlcmd