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
				# ⚠️ 原子预留（2026-11-01）：旧实现「先选 not used → await 探活 → 最后置
				# used=True」两步之间有挂起，两个协程会拿到同一个 sqlor/同一条 cursor：
				# 一方 exit() 关游标后，另一方长任务末尾写就抛 `Cursor closed`；共享游标下
				# execute/fetch 交错还可能串到别的项目/任务的行（多租户数据污染）。
				# 这里先占位（无 await，天然原子），再做可能挂起的探活；失败才丢弃。
				if s.used:
					continue
				s.used = True
				# Skip check_alive if connection was used recently
				idle_time = now - s.use_at
				if idle_time < self.CHECK_ALIVE_THRESHOLD:
					# 便宜探活（无 DB 往返）：transport 已关闭的连接绝不发出。旧实现此分支
					# 直接放行，坏 socket 交付后查询报隐晦错误（2026-10-09 排查）。
					if self._transport_ok(s.sqlor):
						yielded_sqlor = s
						break
					debug('SqlorPool.context: discarding dead connection (transport closed)')
					self._discard_sqlor(s)
					continue
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
					'cursor is None',
				)):
					self._discard_sqlor(yielded_sqlor)
					yielded_sqlor = None
				raise
			finally:
				# Always reset state, even on CancelledError (BaseException in Python 3.8+)
				if yielded_sqlor is not None:
					yielded_sqlor.used = False
					yielded_sqlor.use_at = time.time()

	def _transport_ok(self, sor):
		"""便宜探活：只查 transport 是否已关闭，不做 DB 往返（高频分支用）。"""
		try:
			conn = sor.conn
			if conn is None:
				return False
			writer = getattr(conn, '_writer', None)
			if writer is None:
				return True
			transport = getattr(writer, 'transport', None)
			if transport is None:
				return True
			return not transport.is_closing()
		except Exception:
			return True   # 探活本身出错不误杀，交给完整探活

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
				else:
					# SELECT-only 事务也要结束：rollback 释放 MDL 锁和 MVCC 快照。
					# 否则连接回池后事务未提交，持有 MDL_SHARED 阻塞后续 DDL/元数据操作，
					# 而 lock_wait_timeout 默认 86400(24h)，可卡死 poller 达数小时。
					# (2026-08 生产事故：poller 停摆 15.7h，根因即此)
					await sqlor.rollback()
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
