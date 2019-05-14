
import asyncio
from queue import Queue
from functools import wraps 
import codecs

from appPublic.myImport import myImport
from appPublic.dictObject import DictObject
from appPublic.Singleton import SingletonDecorator
from appPublic.myjson import loadf
from appPublic.jsonConfig import getConfig

from .sor import SQLor
from .mssqlor	import MsSqlor
from .oracleor import Oracleor
from .sqlite3or import SQLite3or
from .mysqlor import MySqlor
from .aiomysqlor import AioMysqlor
from .aiopostgresqlor import AioPostgresqlor


def sqlorFactory(dbdesc):
	driver = dbdesc.get('driver',dbdesc)
	def findSubclass(name,klass):
		for k in klass.__subclasses__():
			if k.isMe(name):
				return k
			k1 = findSubclass(name,k)
			if k1 is not None:
				return k1
		return None
	k = findSubclass(driver,SQLor)
	if k is None:
		return SQLor(dbdesc=dbdesc)
	return k(dbdesc=dbdesc)

def sqlorFromFile(dbdef_file,coding='utf8'):
	dbdef = loadf(dbdef_file)
	return sqlorFactory(dbdef)
	
class LifeConnect:
	__conndict = {}
	def __init__(self,connfunc,kw,use_max=1000,async_mode=False):
		self.connfunc = connfunc
		self.async_mode = async_mode
		self.use_max = use_max
		self.kw = kw
		self.conn = None
	
	def print(self):
		print(self.use_max)
		print(self.conn)

	async def _mkconn(self):
		if self.async_mode:
			self.conn = await self.connfunc(**self.kw)
		else:
			self.conn = self.connfunc(**self.kw)
		self.use_cnt = 0
		self.__conndict[self.conn] = self

	async def use(self):
		if self.conn is None:
			await self._mkconn()
		conn = self.conn
		if await self.testok():
			return conn
		del self.__conndict[conn]
		await self._mkconn()

	@classmethod
	async def free(self,conn):
		lc = self.__conndict[conn]
		lc.use_cnt = lc.use_cnt + 1
		if lc.use_cnt >= lc.use_max:
			await lc.conn.close()
			await lc._mkcomm()
		return lc

	async def testok(self):
		if self.async_mode:
			async with self.conn.cursor() as cur:
				try:
					await cur.execute('select 1 as cnt')
					return True
				except:
					return False
		else:
			cur = self.conn.cursor()
			try:
				cur.execute('select 1 as cnt')
				r = cur.fetchall()
				return True
			except:
				return False
			finally:
				cur.close()
	
class ConnectionPool(object):
	def __init__(self,dbdesc,loop):
		self.dbdesc = dbdesc
		self.async_mode = dbdesc.get('async_mode',False)
		self.loop = loop
		self.driver = myImport(self.dbdesc['driver'])
		self.maxconn = dbdesc.get('maxconn',5)
		self.maxuse = dbdesc.get('maxuse',1000)
		self._pool = Queue(self.maxconn)
		self._fillPool()
		self.using = []
		self.use_cnt = 0
		self.max_use = 1000
	
	def _fillPool(self):
		for i in range(self.maxconn):
			lc = self.connect()
			i = i + 1
	
	def connect(self):
		lc = LifeConnect(self.driver.connect,self.dbdesc['kwargs'],
				use_max=self.maxuse,async_mode=self.async_mode)
		self._pool.put(lc)
		return lc

	def isEmpty(self):
		return self._pool.empty()
	
	def isFull(self):
		return self._pool.full()
		
	async def aquire(self):
		lc = self._pool.get()
		self.using.append(lc)
		conn = await lc.use()
		return conn

	async def release(self,conn):
		lc = await LifeConnect.free(conn)
		self.using = [c for c in self.using if c != lc ]
		self._pool.put(lc)
	
@SingletonDecorator
class DBPools:
	def __init__(self,databases={},max_connect=10,loop=None):
		if loop is None:
			loop = asyncio.get_event_loop()
		self.loop = loop
		self._cpools = {}
		self.databases = databases
	
	def addDatabase(self,name,desc):
		self.databases[name] = desc

	async def getSqlor(self,name):
		desc = self.databases.get(name)
		sor = sqlorFactory(desc)
		sor.name = name
		a,conn,cur = await self._aquireConn(name)
		sor.setCursor(a,conn,cur)
		return sor

	async def freeSqlor(self,sor):
		await self._releaseConn(sor.name,sor.conn,sor.cur)

	async def _aquireConn(self,dbname):
		p = self._cpools.get(dbname)
		if p == None:
			p = ConnectionPool(self.databases.get(dbname),self.loop)
			self._cpools[dbname] = p
		conn = await p.aquire()
		if self.isAsyncDriver(dbname):
			cur = await conn.cursor()
		else:
			cur = conn.cursor()
		return self.isAsyncDriver(dbname),conn,cur
	
	def isAsyncDriver(self,dbname):
		ret = self.databases[dbname].get('async_mode',False)
		return ret

	async def _releaseConn(self,dbname,conn,cur):
		if self.isAsyncDriver(dbname):
			await cur.close()
		else:
			try:
				cur.fetchall()
			except:
				pass
			cur.close()
		p = self._cpools.get(dbname)
		if p == None:
			raise Exception('database (%s) not connected'%dbname)
		await p.release(conn)

	def inSqlor(self,func):
		@wraps(func)
		async def wrap_func(sor,dbname,*args,**kw):
			sor = await self.getSqlor(dbname)
			try:
				ret = await func(sor,dbname,*args,**kw)
				return ret
			except Exception as e:
				print('error',sor)
				raise e
			finally:
				await self.freeSqlor(sor)

		return wrap_func
				
	def runSQL(self,func):
		@wraps(func)
		async def wrap_func(dbname,NS,callback,**kw):
			sor = await self.getSqlor(dbname)
			try:
				desc = func(dbname,NS,callback,**kw)
				ret = await sor.runSQL(desc,NS,callback,**kw)
			except Exception as e:
				print('error:',e)
				raise e
			finally:
				await self.freeSqlor(sor)
				return ret
		return wrap_func

	def runSQLPaging(self,func):
		@wraps(func)
		async def wrap_func(dbname,NS,**kw):
			sor = await self.getSqlor(dbname)
			try:
				desc = func(dbname,NS,**kw)
				total = await sor.record_count(desc,NS)
				recs = await sor.pagingdata(desc,NS)
				data = {
					"total":total,
					"rows":recs
				}
				print(len(recs),'records return')
				return data
			except Exception as e:
				print('error',e)
				raise e
			finally:
				await self.freeSqlor(sor)
		return wrap_func

	def runSQLResultFields(self, func):
		@wraps(func)
		async def wrap_func(dbname,NS,**kw):
			sor = await self.getSqlor(dbname)
			try:
				desc = func(dbname,NS,**kw)
				ret = await sor.resultFields(desc,NS)
				return ret
			except Exception as e:
				print('error=',e)
				raise e
			finally:
				await self.freeSqlor(sor)
		return wrap_func

	async def getTables(self,dbname):
		@self.inSqlor
		async def _getTables(sor,dbname):
			ret = await sor.tables()
			return  ret
		return await _getTables(None,dbname)

	async def getTableFields(self,dbname,tblname):
		@self.inSqlor
		async def _getTableFields(sor,dbname,tblname):
			ret = await sor.fields(tblname)
			return ret
		return await _getTableFields(None,dbname,tblname)

	async def getTablePrimaryKey(self,dbname,tblname):
		@self.inSqlor
		async def _getTablePrimaryKey(sor,dbname,tblname):
			ret = await sor.primary(tblname)
			return  ret
		return await _getTablePrimaryKey(None,dbname,tblname)
		
	async def getTableForignKeys(self,dbname,tblname):
		@self.inSqlor
		async def _getTableForignKeys(sor,dbname,tblname):
			ret = await sor.fkeys(tblname)
			return  ret
		return await _getTableForignKeys(None,dbname,tblname)
	
