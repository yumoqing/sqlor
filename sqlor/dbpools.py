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
from appPublic.log import exception, debug
from appPublic.event_dispatcher import EventDispatcher

import threading
from .sor import SQLor
from .mssqlor	import MsSqlor
from .oracleor import Oracleor
from .sqlite3or import SQLite3or
from .aiosqliteor import Aiosqliteor
from .mysqlor import MySqlor
from .aiopostgresqlor import AioPostgresqlor

# Global semaphore to limit total connections across all pools
_GLOBAL_CONN_LIMIT = 1000  # Leave headroom below MariaDB max_connections=500
_global_conn_sema = None

def _get_global_sema():
	global _global_conn_sema
	if _global_conn_sema is None:
		_global_conn_sema = asyncio.Semaphore(_GLOBAL_CONN_LIMIT)
	return _global_conn_sema

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
		return SQLor(dbdesc=dbdesc.kwargs)
	return k(dbdesc=dbdesc.kwargs)

class SqlorPool:
	# Idle timeout: connections unused for this long will be closed
	IDLE_TIMEOUT = 300  # 5 minutes
	# Only check alive if connection has been idle longer than this
	CHECK_ALIVE_THRESHOLD = 30  # 30 seconds

	def __init__(self, create_func, maxconn=200):
		self.sema = asyncio.Semaphore(maxconn)
		self.create_func = create_func
		self.sqlors = []
	
	async def _new_sqlor(self):
		global_sema = _get_global_sema()
		await global_sema.acquire()
		try:
			sqlor = await self.create_func()
			await sqlor.connect()
			x = DictObject(**{
				'used': True,
				'use_at': time.time(),
				'sqlor':sqlor
			})
			self.sqlors.append(x)
			return x
		except Exception:
			global_sema.release()
			raise

	async def _del_sqlor(self, sor):
		global_sema = _get_global_sema()
		try:
			await sor.exit()
		except:
			pass
		try:
			await sor.close()
		except:
			pass
		global_sema.release()

	def _cleanup_idle(self):
		"""Remove idle connections that exceed IDLE_TIMEOUT."""
		now = time.time()
		to_remove = []
		for s in self.sqlors:
			if not s.used and (now - s.use_at) > self.IDLE_TIMEOUT:
				to_remove.append(s)
		for s in to_remove:
			self.sqlors = [x for x in self.sqlors if x != s]
			# Fire-and-forget close (best effort)
			asyncio.ensure_future(self._del_sqlor(s.sqlor))

	async def test_sqlor(self, sor):
		try:
			await sor.enter()
			await sor.execute(sor.test_sqlstr, {})
			await sor.exit()
			return True
		except:
			await sor.exit()
			return False

	def _discard_sqlor(self, entry):
		"""Remove a broken connection entry from the pool and clean up."""
		if entry in self.sqlors:
			self.sqlors.remove(entry)
		asyncio.ensure_future(self._del_sqlor(entry.sqlor))

	@asynccontextmanager
	async def context(self):
		self._cleanup_idle()
		async with self.sema:
			yielded_sqlor = None
			# Try to find a healthy idle connection
			sqlors = [s for s in self.sqlors if not s.used]
			now = time.time()
			for s in sqlors:
				# Skip check_alive if connection was used recently
				idle_time = now - s.use_at
				if idle_time < self.CHECK_ALIVE_THRESHOLD:
					yielded_sqlor = s
					break
				ok = await self._check_alive(s.sqlor)
				if ok:
					yielded_sqlor = s
					break
				else:
					debug(f'SqlorPool.context: discarding dead connection')
					self._discard_sqlor(s)

			if not yielded_sqlor:
				yielded_sqlor = await self._new_sqlor()
			yielded_sqlor.used = True
			yielded_sqlor.use_at = time.time()
			try:
				yield yielded_sqlor.sqlor
			except (RuntimeError, OSError) as e:
				err_msg = str(e)
				if 'closed' in err_msg or 'handler is closed' in err_msg:
					# Connection died during use — discard it from pool
					self._discard_sqlor(yielded_sqlor)
					yielded_sqlor = None  # prevent finally from resetting
				raise
			except Exception as e:
				err_msg = str(e)
				if any(x in err_msg for x in (
					'Not connected', 'Command Out of Sync',
					'Lost connection', 'Connection reset',
					'Server disconnected',
				)):
					self._discard_sqlor(yielded_sqlor)
					yielded_sqlor = None
				raise
			finally:
				# Always reset state, even on CancelledError (BaseException in Python 3.8+)
				if yielded_sqlor is not None:
					yielded_sqlor.used = False
					yielded_sqlor.use_at = time.time()

	async def _check_alive(self, sor):
		"""Lightweight check: can we still talk to the DB on this connection?"""
		try:
			conn = sor.conn
			if conn is None:
				return False
			# aiomysql: check transport
			writer = getattr(conn, '_writer', None)
			if writer is not None:
				transport = getattr(writer, 'transport', None)
				if transport is not None and transport.is_closing():
					return False
			# Try a lightweight ping
			await sor.enter()
			await sor.execute(sor.test_sqlstr, {})
			await sor.exit()
			return True
		except Exception:
			try:
				await sor.exit()
			except:
				pass
			return False

@SingletonDecorator
class DBPools(EventDispatcher):
	def __init__(self,databases={},max_connect=100,loop=None):
		if loop is None:
			loop = get_event_loop()
		super().__init__()
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
		try:
			desc = self.databases.get(name)
			sor = sqlorFactory(desc)
			await sor.connect()
			return sor
		except Exception as e:
			exception(f'{name=}, {e}')
			raise e

	@asynccontextmanager
	async def sqlorContext(self, name):
		pool = self._pools.get(name)
		if pool is None:
			f = partial(self.getSqlor, name)
			pool = SqlorPool(f)
			self._pools[name] = pool
		self.e_except = None
		sqlor = None
		async with pool.context() as sqlor:
			try:
				sqlor.dbpools = self
				await sqlor.enter()
				yield sqlor
				if sqlor and sqlor.dataChanged:
					await sqlor.commit()
			except BaseException as e:
				self.e_except = e
				if not isinstance(e, asyncio.CancelledError):
					cb = format_exc()
					exception(f'sqlorContext():EXCEPTION{e}, {cb}')
				try:
					await sqlor.rollback()
				except:
					pass
				raise
			finally:
				# Always call exit(), even on CancelledError (BaseException in Python 3.8+)
				if sqlor:
					try:
						await sqlor.exit()
					except:
						pass
	
	def get_exception(self):
		return self.e_except

@asynccontextmanager
async def get_sor_context(env, modulename, errorback=None):
	db = DBPools()
	dbname = env.get_module_dbname(modulename)
	async with db.sqlorContext(dbname) as sor:
		yield sor
	if db.e_except:
		if errorback:
			await errorback(db.e_except)
		else:
			raise db.e_except
