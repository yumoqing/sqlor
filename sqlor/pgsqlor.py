# -*- coding:utf8 -*-
import asyncpg
from appPublic.argsConvert import ArgsConvert, ConditionConvert
from .sor import SQLor
from .ddl_template_pgsql import pgsql_ddl_tmpl  # 需要提供 PostgreSQL DDL 模板

class PgSqlor(SQLor):
	ddl_template = pgsql_ddl_tmpl

	db2modelTypeMapping = {
		'smallint': 'short',
		'integer': 'long',
		'bigint': 'long',
		'decimal': 'float',
		'numeric': 'float',
		'real': 'float',
		'double precision': 'float',
		'serial': 'long',
		'bigserial': 'long',
		'varchar': 'str',
		'char': 'char',
		'text': 'text',
		'bytea': 'bin',
		'date': 'date',
		'time': 'time',
		'timestamp': 'datetime',
		'timestamptz': 'datestamp',
		'boolean': 'short',
		'json': 'text',
		'jsonb': 'text',
	}

	model2dbTypemapping = {
		'date': 'date',
		'time': 'time',
		'timestamp': 'timestamp',
		'str': 'varchar',
		'char': 'char',
		'short': 'smallint',
		'long': 'bigint',
		'float': 'double precision',
		'text': 'text',
		'bin': 'bytea',
		'file': 'bytea',
	}

	@classmethod
	def isMe(cls, name):
		return name.lower() in ['pgsql', 'postgres', 'postgresql']

	def grammar(self):
		return {
			# PostgreSQL 支持 window/CTE 等，可在这里扩展
			'select': 'select',
		}

	def placeHolder(self, varname, pos=None):
		"""PostgreSQL 使用 $1, $2 作为占位符"""
		if varname == '__mainsql__':
			return ''
		if pos is not None:
			return f"${pos + 1}"
		return '$1'

	def dataConvert(self, dataList):
		if isinstance(dataList, dict):
			return tuple(dataList.values())
		else:
			return tuple(i['value'] for i in dataList)

	def pagingSQLmodel(self):
		"""分页模板"""
		return """select * from (%s) as A order by $[sort]$ offset $[from_line]$ limit $[rows]$"""

	def tablesSQL(self):
		sqlcmd = f"""
			SELECT 
				lower(tablename) as name,
				obj_description(format('%s.%s', schemaname, tablename)::regclass) as title
			FROM pg_tables
			WHERE schemaname NOT IN ('pg_catalog', 'information_schema');
		"""
		return sqlcmd

	def fieldsSQL(self, tablename=None):
		sqlcmd = f"""
			SELECT 
				lower(a.attname) as name,
				format_type(a.atttypid, a.atttypmod) as type,
				CASE 
					WHEN a.atttypmod > 0 THEN a.atttypmod - 4
					ELSE NULL
				END as length,
				NULL as "dec",
				CASE WHEN a.attnotnull THEN 'no' ELSE 'yes' END as nullable,
				col_description(a.attrelid, a.attnum) as title,
				lower(c.relname) as table_name
			FROM pg_attribute a
			JOIN pg_class c ON a.attrelid = c.oid
			JOIN pg_namespace n ON c.relnamespace = n.oid
			WHERE a.attnum > 0 AND NOT a.attisdropped
			  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
		"""
		if tablename:
			sqlcmd += f" AND lower(c.relname) = '{tablename.lower()}'"
		return sqlcmd

	def fkSQL(self, tablename=None):
		sqlcmd = f"""
			SELECT
				con.conname AS constraint_name,
				nsp.nspname AS schema_name,
				rel.relname AS child_table,
				att.attname AS child_column,
				frel.relname AS parent_table,
				fatt.attname AS parent_column,
				con.confupdtype AS update_rule,
				con.confdeltype AS delete_rule
			FROM pg_constraint con
			JOIN pg_class rel ON rel.oid = con.conrelid
			JOIN pg_class frel ON frel.oid = con.confrelid
			JOIN pg_attribute att ON att.attrelid = con.conrelid AND att.attnum = ANY(con.conkey)
			JOIN pg_attribute fatt ON fatt.attrelid = con.confrelid AND fatt.attnum = ANY(con.confkey)
			JOIN pg_namespace nsp ON nsp.oid = con.connamespace
			WHERE con.contype = 'f'
		"""
		if tablename:
			sqlcmd += f" AND lower(rel.relname) = '{tablename.lower()}'"
		return sqlcmd

	def pkSQL(self, tablename=None):
		sqlcmd = f"""
			SELECT a.attname as name
			FROM pg_index i
			JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
			WHERE i.indrelid = '{tablename.lower()}'::regclass
			  AND i.indisprimary;
		"""
		return sqlcmd

	def indexesSQL(self, tablename=None):
		sqlcmd = f"""
			SELECT
				lower(i.relname) as index_name,
				CASE WHEN ix.indisunique THEN 'unique' ELSE '' END as is_unique,
				lower(a.attname) as column_name
			FROM pg_class t,
				 pg_class i,
				 pg_index ix,
				 pg_attribute a
			WHERE
				t.oid = ix.indrelid
				AND i.oid = ix.indexrelid
				AND a.attrelid = t.oid
				AND a.attnum = ANY(ix.indkey)
				AND t.relkind = 'r'
		"""
		if tablename:
			sqlcmd += f" AND lower(t.relname) = '{tablename.lower()}'"
		return sqlcmd

	async def connect(self):
		"""建立数据库连接"""
		dbdesc = self.dbdesc
		self.conn = await asyncpg.connect(
			user=dbdesc.get('user'),
			password=dbdesc.get('password'),
			database=dbdesc.get('db'),
			host=dbdesc.get('host', 'localhost'),
			port=dbdesc.get('port', 5432),
		)
		self.dbname = dbdesc.get('db')

	async def close(self):
		await self.conn.close()

	async def enter(self):
		"""开启事务或获取游标（asyncpg 无游标，直接用连接执行）"""
		self.cur = self.conn  # 保留接口一致性

	async def exit(self):
		"""释放资源"""
		self.cur = None

