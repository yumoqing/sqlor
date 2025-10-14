# -*- coding:utf8 -*-
from appPublic.argsConvert import ArgsConvert, ConditionConvert
from .sor import SQLor
from .const import ROWS
from .ddl_template_mysql import mysql_ddl_tmpl  # ✅ 直接复用 MySQL 模板

class TiDBor(SQLor):
	ddl_template = mysql_ddl_tmpl

	db2modelTypeMapping = {
		'tinyint': 'short',
		'smallint': 'short',
		'mediumint': 'long',
		'int': 'long',
		'bigint': 'long',
		'decimal': 'float',
		'double': 'float',
		'float': 'float',
		'char': 'char',
		'varchar': 'str',
		'text': 'text',
		'blob': 'text',
		'longtext': 'text',
		'binary': 'text',
		'date': 'date',
		'time': 'time',
		'datetime': 'datetime',
		'timestamp': 'datestamp',
		'year': 'short',
	}

	model2dbTypemapping = {
		'date': 'date',
		'time': 'time',
		'timestamp': 'timestamp',
		'str': 'varchar',
		'char': 'char',
		'short': 'int',
		'long': 'bigint',
		'float': 'double',
		'text': 'longtext',
		'bin': 'longblob',
		'file': 'longblob',
	}

	@classmethod
	def isMe(self, name):
		# ✅ 允许 "tidb" 或 "mysql"（因为 TiDB 兼容 MySQL 协议）
		return name.lower() == 'tidb'

	def grammar(self):
		return {
			'select': 'select * from {table} where {condition}',
		}

	def placeHolder(self, varname, pos=None):
		if varname == '__mainsql__':
			return ''
		return '%s'

	def dataConvert(self, dataList):
		if isinstance(dataList, dict):
			d = [i for i in dataList.values()]
		else:
			d = [i['value'] for i in dataList]
		return tuple(d)

	def pagingSQLmodel(self):
		# ✅ 与 MySQL 一样
		return """SELECT * FROM (%s) AS A ORDER BY $[sort]$ LIMIT $[from_line]$, $[rows]$"""

	def tablesSQL(self):
		# ✅ TiDB 支持 INFORMATION_SCHEMA.TABLES
		dbname = self.dbdesc.get('dbname', 'unknown')
		sqlcmd = f"""SELECT lower(TABLE_NAME) AS name, TABLE_COMMENT AS title
					 FROM INFORMATION_SCHEMA.TABLES
					 WHERE TABLE_SCHEMA = '{dbname}'"""
		return sqlcmd

	def fieldsSQL(self, tablename=None):
		dbname = self.dbdesc.get('dbname', 'unknown').lower()
		sqlcmd = f"""
SELECT 
	lower(column_name) AS name,
	data_type AS type,
	CASE WHEN character_maximum_length IS NULL THEN NUMERIC_PRECISION
		 ELSE character_maximum_length END AS length,
	NUMERIC_SCALE AS dec,
	lower(is_nullable) AS nullable,
	column_comment AS title,
	lower(table_name) AS table_name
FROM information_schema.columns 
WHERE lower(TABLE_SCHEMA) = '{dbname}'
"""
		if tablename:
			sqlcmd += f"AND lower(table_name) = '{tablename.lower()}';"
		return sqlcmd

	def fkSQL(self, tablename=None):
		# ✅ TiDB 兼容 MySQL 的 FK 元信息
		dbname = self.dbdesc.get('dbname', 'unknown').lower()
		sqlcmd = f"""
SELECT 
	C.TABLE_SCHEMA AS owner,
	C.REFERENCED_TABLE_NAME AS parent_table,
	C.REFERENCED_COLUMN_NAME AS parent_column,
	C.TABLE_NAME AS child_table,
	C.COLUMN_NAME AS child_column,
	C.CONSTRAINT_NAME AS constraint_name
FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE C
WHERE C.REFERENCED_TABLE_NAME IS NOT NULL
  AND C.TABLE_SCHEMA = '{dbname}'
"""
		if tablename:
			sqlcmd += f"AND C.TABLE_NAME = '{tablename.lower()}';"
		return sqlcmd

	def pkSQL(self, tablename=None):
		return f"""SELECT column_name AS name 
FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
WHERE table_name='{tablename.lower()}' AND constraint_name='PRIMARY'"""

	def indexesSQL(self, tablename=None):
		dbname = self.dbdesc.get('dbname', 'unknown')
		sqlcmd = f"""SELECT DISTINCT
	lower(index_name) AS index_name,
	CASE NON_UNIQUE WHEN 0 THEN 'unique' ELSE '' END AS is_unique,
	lower(column_name) AS column_name
FROM information_schema.statistics
WHERE table_schema = '{dbname}'"""
		if tablename:
			sqlcmd += f" AND table_name = '{tablename.lower()}'"
		return sqlcmd

