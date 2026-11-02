# -*- coding:utf8 -*-
from appPublic.argsConvert import ArgsConvert,ConditionConvert
import inspect
import aiomysql
from .sor import SQLor
from .ddl_template_mysql import mysql_ddl_tmpl
class MySqlor(SQLor):
	ddl_template = mysql_ddl_tmpl
	db2modelTypeMapping = {
		'tinyint':'short',
		'smallint':'short',
		'mediumint':'long',
		'int':'long',
		'bigint':'long',
		'decimal':'float',
		'double':'float',
		'float':'float',
		'char':'char',
		'varchar':'str',
		'tinyblob':'text',
		'tinytext':'text',
		'mediumblob':'text',
		'mediumtext':'text',
		'blob':'text',
		'text':'text',
		'mediumblob':'text',
		'mediumtext':'text',
		'longblob':'bin',
		'longtext':'text',
		'barbinary':'text',
		'binary':'text',
		'date':'date',
		'time':'time',
		'datetime':'datetime',
		'timestamp':'datestamp',
		'year':'short',
	}
	model2dbTypemapping = {
		'date':'date',
		'time':'date',
		'timestamp':'timestamp',
		'str':'varchar',
		'char':'char',
		'short':'int',
		'long':'bigint',
		'float':'double',
		'text':'longtext',
		'bin':'longblob',
		'file':'longblob',
	}
	@classmethod
	def isMe(self,name):
		if name in ['mysql', 'aiosqlor', 'tidb']:
			return True
		return False
	
	def grammar(self):
		return {
			'select':select_stmt,
		}
		
	def placeHolder(self,varname,pos=None):
		if varname=='__mainsql__' :
			return ''
		return '%s'
	
	def dataConvert(self,dataList):
		if type(dataList) == type({}):
			d = [ i for i in dataList.values()]
		else:
			d = [ i['value'] for i in dataList]
		return tuple(d)

	def pagingSQLmodel(self):
		return """select *, row_number() over(order by $[sort]$) as row_num_ from (%s) A limit $[from_line]$, $[rows]$"""
		return """select * from (%s) A order by $[sort]$
limit $[from_line]$,$[rows]$"""

	def tablesSQL(self):
		sqlcmd = """SELECT lower(TABLE_NAME) as name, lower(TABLE_COMMENT) as title FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '%s'""" % self.dbname
		return sqlcmd
	
	def fieldsSQL(self,tablename=None):
		sqlcmd="""
 select 
	lower(column_name) as name,
	data_type as type,
	case when character_maximum_length is null then  NUMERIC_PRECISION
		else character_maximum_length end
	as length,
	NUMERIC_SCALE as 'dec',
	lower(is_nullable) as nullable,
	column_comment as title,
	lower(table_name) as table_name
 from information_schema.columns where lower(TABLE_SCHEMA) = '%s' """ % self.dbname
		if tablename is not None:
			sqlcmd = sqlcmd + """and lower(table_name)='%s';""" % tablename.lower()
		return sqlcmd

	def fkSQL(self,tablename=None):
		sqlcmd = """SELECT C.TABLE_SCHEMA            拥有者,
           C.REFERENCED_TABLE_NAME  父表名称 ,
           C.REFERENCED_COLUMN_NAME 父表字段 ,
           C.TABLE_NAME             子表名称,
           C.COLUMN_NAME            子表字段,
           C.CONSTRAINT_NAME        约束名,
           T.TABLE_COMMENT          表注释,
           R.UPDATE_RULE            约束更新规则,
           R.DELETE_RULE            约束删除规则
      FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE C
      JOIN INFORMATION_SCHEMA. TABLES T
        ON T.TABLE_NAME = C.TABLE_NAME
      JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS R
        ON R.TABLE_NAME = C.TABLE_NAME
       AND R.CONSTRAINT_NAME = C.CONSTRAINT_NAME
       AND R.REFERENCED_TABLE_NAME = C.REFERENCED_TABLE_NAME
      WHERE C.REFERENCED_TABLE_NAME IS NOT NULL ;
		and C.TABLE_SCHEMA = '%s'
""" % self.dbname
		if tablename is not None:
			sqlcmd = sqlcmd + " and C.REFERENCED_TABLE_NAME = '%s'" % tablename.lower()
		return sqlcmd

	def pkSQL(self,tablename=None):
		sqlcmd = """SELECT distinct column_name as name FROM INFORMATION_SCHEMA.`KEY_COLUMN_USAGE` WHERE table_name='%s' AND constraint_name='PRIMARY'
""" % tablename.lower()
		return sqlcmd

	def indexesSQL(self,tablename=None):
		sqlcmd = """SELECT DISTINCT
    lower(index_name) as index_name,
	case NON_UNIQUE
		when 1 then 'unique'
	else ''
	end as is_unique,
	lower(column_name) as column_name
FROM
    information_schema.statistics
WHERE
    table_schema = '%s'""" % self.dbname
		if tablename is not None:
			sqlcmd = sqlcmd + """	AND table_name = '%s'""" % tablename.lower()
		return sqlcmd

	async def connect(self):
		"""建立连接。若已有连接，先关掉再建，避免重连时服务端连接泄漏。

		历史坑（2026-11-02 生产事故）：enter() 在 commit 失败或连接判死时会调用本方法重连，
		旧实现直接覆盖 self.conn，旧连接(可能仍占着服务端线程/事务锁)从此失去引用，
		只能等进程退出才释放。取消/超时是高频路径，一次取消泄漏一条 → 一晚上打满
		max_connections，全站 500。故此处必须显式关闭旧连接。
		"""
		dbdesc = self.dbdesc
		old = getattr(self, 'conn', None)
		if old is not None:
			try:
				res = old.close()
				if inspect.isawaitable(res):
					await res
			except BaseException:
				pass
			finally:
				self.conn = None
		self.conn = await aiomysql.connect(**dbdesc)
		self.dbname = dbdesc.get('db')

	async def close(self):
		"""真正关闭游标与连接（2026-11-01 修复：旧实现必抛，连接从未被关闭）。

		旧代码两处错：
		  `await self.cursor.close()` —— self.cursor 是 SQLor 的**方法**，不是游标对象；
		  `await self.conn.close()`   —— aiomysql 的 Connection.close() 是**同步函数**，
		      await None 抛 TypeError，且异常不被吞 → 连接只靠 GC 回收。
		现在：先关游标（用 self.cur），再同步关连接（可等待才 await）。
		取消安全：关游标时被 CancelledError 打断，也**保证** conn 被关闭，然后把取消重新抛出。
		"""
		cur = getattr(self, 'cur', None)
		_pending = None
		if cur is not None:
			try:
				res = cur.close()
				if inspect.isawaitable(res):
					await res
			except BaseException as _e:      # 取消/超时也不能漏关 conn
				_pending = _e
			finally:
				try:
					self.cur = None
				except Exception:
					pass
		conn = getattr(self, 'conn', None)
		if conn is not None:
			try:
				res = conn.close()
				if inspect.isawaitable(res):
					await res
			except BaseException:
				pass
			finally:
				try:
					self.conn = None
				except Exception:
					pass
		if _pending is not None:
			raise _pending

	def _conn_alive(self):
		"""底层连接是否仍可用（None / transport 已关闭 == 不可用）。"""
		conn = self.conn
		if conn is None:
			return False
		writer = getattr(conn, '_writer', None)
		transport = getattr(writer, 'transport', None) if writer is not None else None
		if transport is not None and transport.is_closing():
			return False
		return True

	async def enter(self):
		if not self._conn_alive():
			await self.connect()
		try:
			await self.conn.commit()
		except Exception:
			# commit 时才发现连接已死 → 重连一次再 commit
			await self.connect()
			await self.conn.commit()
		self.cur = await self.conn.cursor()
		
	async def exit(self):
		try:
			await self.cur.fetchall()
			await self.cur.close()
		except:
			pass
		self.cur = None

