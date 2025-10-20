# -*- coding:utf8 -*-
import duckdb
from appPublic.argsConvert import ArgsConvert, ConditionConvert
from .sor import SQLor
from .const import ROWS
from .ddl_template_duckdb import duckdb_ddl_tmpl

class DuckDBor(SQLor):
	"""
	sqlor 方言适配：DuckDB
	"""
	ddl_template = duckdb_ddl_tmpl

	db2modelTypeMapping = {
		'boolean': 'short',
		'smallint': 'short',
		'integer': 'long',
		'bigint': 'long',
		'double': 'float',
		'float': 'float',
		'real': 'float',
		'varchar': 'str',
		'char': 'char',
		'string': 'str',
		'blob': 'bin',
		'date': 'date',
		'timestamp': 'datetime',
	}

	model2dbTypemapping = {
		'date': 'date',
		'time': 'time',
		'timestamp': 'timestamp',
		'str': 'varchar',
		'char': 'char',
		'short': 'integer',
		'long': 'bigint',
		'float': 'double',
		'text': 'varchar',
		'bin': 'blob',
		'file': 'blob',
	}

	@classmethod
	def isMe(cls, name):
		return name.lower() in ['duckdb']

	def grammar(self):
		# duckdb 使用标准 SQL，通常不需要特殊 select_stmt
		return {}

	def placeHolder(self, varname, pos=None):
		if varname == '__mainsql__':
			return ''
		return '?'

	def dataConvert(self, dataList):
		if isinstance(dataList, dict):
			d = list(dataList.values())
		else:
			d = [i['value'] for i in dataList]
		return tuple(d)

	def pagingSQLmodel(self):
		# DuckDB 兼容标准 SQL LIMIT/OFFSET
		return """SELECT * FROM (%s) AS A ORDER BY $[sort]$ LIMIT $[rows]$ OFFSET $[from_line]$"""

	def tablesSQL(self):
		# DuckDB 不支持 INFORMATION_SCHEMA.TABLES 完全一致的结构
		sqlcmd = """
		SELECT lower(name) AS name, '' AS title
		FROM duckdb_tables()
		WHERE database_name = current_database();
		"""
		return sqlcmd

	def fieldsSQL(self, tablename=None):
		sqlcmd = """
		SELECT 
			lower(column_name) AS name,
			lower(column_type) AS type,
			NULL AS length,
			NULL AS dec,
			'YES' AS nullable,
			'' AS title,
			lower(table_name) AS table_name
		FROM duckdb_columns()
		WHERE database_name = current_database()
		"""
		if tablename is not None:
			sqlcmd += f" AND lower(table_name) = '{tablename.lower()}'"
		return sqlcmd

	def fkSQL(self, tablename=None):
		# DuckDB 暂不支持外键约束查询
		sqlcmd = "SELECT NULL AS constraint_name WHERE 1=0"
		return sqlcmd

	def pkSQL(self, tablename=None):
		sqlcmd = f"""
		SELECT column_name AS name
		FROM duckdb_constraints()
		WHERE constraint_type = 'PRIMARY KEY'
		  AND table_name = '{tablename.lower()}'
		"""
		return sqlcmd

	def indexesSQL(self, tablename=None):
		sqlcmd = """
		SELECT
			lower(index_name) AS index_name,
			'' AS is_unique,
			lower(column_name) AS column_name
		FROM duckdb_indexes()
		WHERE database_name = current_database()
		"""
		if tablename is not None:
			sqlcmd += f" AND lower(table_name) = '{tablename.lower()}'"
		return sqlcmd

	async def connect(self):
		self.conn = duckdb.connect(self.dbdesc.dbfile)
		self.cur = self.conn
		self.dbname = None
	
	async def close(self):
		self.conn.close()
	
	def unpassword(self):
		pass


