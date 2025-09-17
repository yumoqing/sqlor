
import asyncio
from traceback import format_exc
from functools import wraps 
import codecs

from contextlib import asynccontextmanager

from appPublic.myImport import myImport
from appPublic.dictObject import DictObject
from appPublic.Singleton import SingletonDecorator
from appPublic.myjson import loadf
from appPublic.jsonConfig import getConfig
from appPublic.rc4 import unpassword
from appPublic.log import exception

import threading
from .sor import SQLor
from .mssqlor	import MsSqlor
from .oracleor import Oracleor
from .sqlite3or import SQLite3or
from .aiosqliteor import Aiosqliteor
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
		self.e_except = None
		# self.lock = asyncio.Lock()
		# self.lockstatus()
	
	def lockstatus(self):
		return
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
		"""
		with await self.lock:
			self.connectObject[lc.conn] = lc
		"""
		self.connectObject[lc.conn] = lc
		return conn

	async def release(self,conn):
		lc = None
		"""
		with await self.lock:
			lc = self.connectObject.get(conn,None)
			del self.connectObject[conn]
		"""
		lc = self.connectObject.get(conn,None)
		del self.connectObject[conn]
		await self._pool.put(lc)
	
@SingletonDecorator
class DBPools:
	def __init__(self,databases={},max_connect=100,loop=None):
		if loop is None:
			loop = asyncio.get_event_loop()
		self.loop = loop
		self.max_connect = max_connect
		self.sema = asyncio.Semaphore(max_connect)
		self._cpools = {}
		self.databases = databases
		self.meta = {}

	def get_dbname(self, name):
		desc = self.database.get(name)
		if not desc:
			return None
		return desc.get('dbname')
	
	def addDatabase(self,name,desc):
		self.databases[name] = desc

	async def getSqlor(self,name):
		await self.sema.acquire()
		desc = self.databases.get(name)
		sor = sqlorFactory(desc)
		sor.name = name
		a,conn,cur = await self._aquireConn(name)
		sor.setCursor(a,conn,cur)
		return sor

	async def freeSqlor(self,sor):
		await self._releaseConn(sor.name,sor.conn,sor.cur)
		self.sema.release()

	@asynccontextmanager
	async def sqlorContext(self,name):
		self.e_except = None
		sqlor = await self.getSqlor(name)
		try:
			yield sqlor
		except Exception as e:
			self.e_except = e
			cb = format_exc()
			exception(f'sqlorContext():EXCEPTION{e}, {cb}')
			if sqlor and sqlor.dataChanged:
				await sqlor.rollback()
		finally:
			if sqlor and sqlor.dataChanged:
				await sqlor.commit()
			await self.freeSqlor(sqlor)
	
	async def _aquireConn(self,dbname):
		"""
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
		"""
		dbdesc = self.databases.get(dbname)
		driver = myImport(dbdesc['driver'])
		conn = None
		cur = None
		desc = dbdesc['kwargs'].copy()
		pw = desc.get('password')
		if pw:
			desc['password'] = unpassword(pw)
		if self.isAsyncDriver(dbname):
			if dbdesc['driver'] == 'sqlite3':
				conn = await driver.connect(desc['dbname'])
			else:
				conn = await driver.connect(**desc)
			cur = await conn.cursor()
			return True,conn,cur
		else:
			if dbdesc['driver'] == 'sqlite3':
				conn = driver.connect(desc['dbname'])
			else:
				conn = driver.connect(**desc)
			cur = conn.cursor()
			return False,conn,cur

	def isAsyncDriver(self,dbname):
		ret = self.databases[dbname].get('async_mode',False)
		return ret

	async def _releaseConn(self,dbname,conn,cur):
		"""
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
		"""
		if self.isAsyncDriver(dbname):
			try:
				await cur.close()
			except:
				pass
		else:
			try:
				cur.fetchall()
			except:
				pass
			cur.close()
		conn.close()

