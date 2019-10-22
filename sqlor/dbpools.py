
import asyncio
from functools import wraps 
import codecs

from appPublic.myImport import myImport
from appPublic.dictObject import DictObject
from appPublic.Singleton import SingletonDecorator
from appPublic.myjson import loadf
from appPublic.jsonConfig import getConfig

import threading
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
	def __init__(self,connfunc,kw,use_max=1000,async_mode=False):
		self.connfunc = connfunc
		self.async_mode = async_mode
		self.use_max = use_max
		self.kw = kw
		self.conn = None
		self.used = False
	
	def print(self):
		print(self.use_max)
		print(self.conn)

	async def _mkconn(self):
		if self.async_mode:
			self.conn = await self.connfunc(**self.kw)
		else:
			self.conn = self.connfunc(**self.kw)
		self.use_cnt = 0

	async def use(self):
		if self.conn is None:
			await self._mkconn()
		wait_time = 0.2
		loop_cnt = 4
		while loop_cnt > 0:
			if await self.testok():
				return self.conn
			await asyncio.sleep(wait_time)
			wait_time = wait_time + 0.4
			loop_cnt = loop_cnt - 1
			try:
				await self.conn.close()
			except:
				pass
			self.conn = None
			await self._mkconn()
		raise Exception('database connect break')

	async def free(self,conn):
		self.use_cnt = self.use_cnt + 1
		return 
		if self.use_cnt >= self.use_max:
			await self.conn.close()
			await self._mkcomm()

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
		self._pool = asyncio.Queue(self.maxconn)
		self.connectObject = {}
		self.use_cnt = 0
		self.max_use = 1000
		self.lock = asyncio.Lock()
		# self.lockstatus()
	
	def lockstatus(self):
		self.loop.call_later(5,self.lockstatus)
		print('--lock statu=',self.lock.locked(),
				'--pool empty()=',self._pool.empty(),
				'--full()=',self._pool.full()
			)

	async def _fillPool(self):
		for i in range(self.maxconn):
			lc = await self.connect()
			i = i + 1
	
	async def connect(self):
		lc = LifeConnect(self.driver.connect,self.dbdesc['kwargs'],
				use_max=self.maxuse,async_mode=self.async_mode)
		await self._pool.put(lc)
		return lc

	def isEmpty(self):
		return self._pool.empty()
	
	def isFull(self):
		return self._pool.full()
		
	async def aquire(self):
		lc = await self._pool.get()
		conn = await lc.use()
		with await self.lock:
			self.connectObject[lc.conn] = lc
		return conn

	async def release(self,conn):
		lc = None
		with await self.lock:
			lc = self.connectObject.get(conn,None)
			del self.connectObject[conn]
		await self._pool.put(lc)
	
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
			await p._fillPool()
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

	async def useOrGetSor(self,dbname,**kw):
			commit = False
			if kw.get('sor'):
				sor = kw['sor']
			else:
				sor = await self.getSqlor(dbname)
				commit = True
			return sor, commit
		
	def inSqlor(self,func):
		@wraps(func)
		async def wrap_func(dbname,NS,*args,**kw):
			sor, commit = await self.useOrGetSor(dbname, **kw)
			kw['sor'] = sor
			try:
				ret = await func(dbname,NS,*args,**kw)
				if not commit:
					return ret
				try:
					await sor.conn.commit()
				except:
					pass
				return ret
			except Exception as e:
				print('error',sor)
				if not commit:
					raise e
				try:
					await sor.conn.rollback()
				except:
					pass
				raise e
			finally:
				if commit:
					await self.freeSqlor(sor)

		return wrap_func
				
	def runSQL(self,func):
		@wraps(func)
		async def wrap_func(dbname,NS,*args,**kw):
			sor, commit = await self.useOrGetSor(dbname,**kw)
			kw['sor'] = sor
			ret = None
			try:
				desc = await func(dbname,NS,*args,**kw)
				callback = kw.get('callback',None)
				kw1 = {}
				[  kw1.update({k:v}) for k,v in kw.items() if k!='callback' ]
				ret = await sor.runSQL(desc,NS,callback,**kw1)
				if commit:
					try:
						await sor.conn.commit()
					except:
						pass
				if NS.get('dummy'):
					return NS['dummy']
				else:
					return []
			except Exception as e:
				print('error:',e)
				if not commit:
					raise e
				try:
					await sor.conn.rollback()
				except:
					pass
				raise e
			finally:
				if commit:
					await self.freeSqlor(sor)
		return wrap_func

	def runSQLPaging(self,func):
		@wraps(func)
		async def wrap_func(dbname,NS,*args,**kw):
			sor, commit = await self.useOrGetSor(dbname,**kw)
			kw['sor'] = sor
			try:
				desc = await func(dbname,NS,*args,**kw)
				total = await sor.record_count(desc,NS)
				recs = await sor.pagingdata(desc,NS)
				data = {
					"total":total,
					"rows":recs
				}
				return DictObject(**data)
			except Exception as e:
				print('error',e)
				raise e
			finally:
				if commit:
					await self.freeSqlor(sor)
		return wrap_func

	async def runSQLResultFields(self, func):
		@wraps(func)
		async def wrap_func(dbname,NS,*args,**kw):
			sor, commit = await self.useOrGetSor(dbname,**kw)
			kw['sor'] = sor
			try:
				desc = await func(dbname,NS,*args,**kw)
				ret = await sor.resultFields(desc,NS)
				return ret
			except Exception as e:
				print('error=',e)
				raise e
			finally:
				if commit:
					await self.freeSqlor(sor)
		return wrap_func

	async def getTables(self,dbname,**kw):
		@self.inSqlor
		async def _getTables(dbname,NS,**kw):
			sor = kw['sor']
			ret = await sor.tables()
			return  ret
		return await _getTables(dbname,{},**kw)

	async def getTableFields(self,dbname,tblname,**kw):
		@self.inSqlor
		async def _getTableFields(dbname,NS,tblname,**kw):
			sor = kw['sor']
			ret = await sor.fields(tblname)
			return ret
		return await _getTableFields(dbname,{},tblname,**kw)

	async def getTablePrimaryKey(self,dbname,tblname,**kw):
		@self.inSqlor
		async def _getTablePrimaryKey(dbname,NS,tblname,**kw):
			sor = kw['sor']
			ret = await sor.primary(tblname)
			return  ret
		return await _getTablePrimaryKey(dbname,{},tblname,**kw)
		
	async def getTableIndexes(self,dbname,tblname,**kw):
		@self.inSqlor
		async def _getTablePrimaryKey(dbname,NS,tblname,**kw):
			sor = kw['sor']
			ret = await sor.indexes(tblname)
			return  ret
		return await _getTablePrimaryKey(dbname,{},tblname,**kw)

	async def getTableForignKeys(self,dbname,tblname,**kw):
		@self.inSqlor
		async def _getTableForignKeys(dbname,NS,tblname,**kw):
			sor = kw['sor']
			ret = await sor.fkeys(tblname)
			return ret
		return await _getTableForignKeys(dbname,{},tblname,**kw)
	
