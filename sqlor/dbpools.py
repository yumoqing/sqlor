import time
import asyncio
from traceback import format_exc
from functools import wraps, partial
import codecs

from contextlib import asynccontextmanager

from appPublic.worker import get_event_loop
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
	return k(dbdesc=dbdesc.kwargs)

class SqlorPool:
	def __init__(self, create_func, maxconn=100):
		self.sema = asyncio.Semaphore(maxconn)
		self.create_func = create_func
		self.sqlors = []
	
	async def _new_sqlor(self):
		sqlor = await self.create_func()
		await sqlor.connect()
		x = DictObject(**{
			'used': True,
			'use_at': time.time(),
			'sqlor':sqlor
		})
		self.sqlors.append(x)
		return x

	async def _del_sqlor(self, sor):
		try:
			await sor.exit()
		except:
			pass
		try:
			await sor.close()
		except:
			pass

	async def test_sqlor(self, sor):
		try:
			await sor.enter()
			await sor.execute(sor.test_sqlstr, {})
			await sor.exit()
			return True
		except:
			await sor.exit()
			return False

	@asynccontextmanager
	async def context(self):
		async with self.sema:
			sqlors = [s for s in self.sqlors]
			yielded_sqlor = None
			for s in sqlors:
				if not s.used:
					flg = await self.test_sqlor(s.sqlor)
					if flg:
						yielded_sqlor = s
					else:
						await self._del_sqlor(s.sqlor)
						self.sqlors = [ x for x in self.sqlors if x != s ]
			if not yielded_sqlor:
				yielded_sqlor = await self._new_sqlor()
			yielded_sqlor.used = True
			yielded_sqlor.use_at = time.time()
			yield yielded_sqlor.sqlor
			yielded_sqlor.used = False
			

@SingletonDecorator
class DBPools:
	def __init__(self,databases={},max_connect=100,loop=None):
		if loop is None:
			loop = get_event_loop()
		self.loop = loop
		self.max_connect = max_connect
		self._pools = {}
		self.databases = databases
		self.meta = {}

	def get_dbname(self, name):
		desc = self.databases.get(name)
		if not desc:
			return None
		return desc.get('dbname')
	
	def addDatabase(self, name, desc):
		self.databases[name] = desc

	async def getSqlor(self, name):
		desc = self.databases.get(name)
		sor = sqlorFactory(desc)
		await sor.connect()
		return sor

	@asynccontextmanager
	async def sqlorContext(self, name):
		pool = self._pools.get(name)
		if pool is None:
			f = partial(self.getSqlor, name)
			pool = SqlorPool(f)
			self._pools[name] = pool
		self.e_except = None
		sqlor = None
		try:
			async with pool.context() as sqlor:
				await sqlor.enter()
				yield sqlor
				await sqlor.exit()
			if sqlor and sqlor.dataChanged:
				await sqlor.commit()
		except Exception as e:
			self.e_except = e
			cb = format_exc()
			exception(f'sqlorContext():EXCEPTION{e}, {cb}')
			if sqlor and sqlor.dataChanged:
				await sqlor.rollback()
	
	def get_exception(self):
		return self.e_except

