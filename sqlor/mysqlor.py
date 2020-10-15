# -*- coding:utf8 -*-
from mysql import connector
from appPublic.argsConvert import ArgsConvert,ConditionConvert

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
		if  name=='mysql.connector':
			return True
		if name=='aiomysql':
			return True
		return False
	
	def grammar(self):
		return {
			'select':select_stmt,
		}
		
	def _opendb(self):
		self.conn = connector.connect(**self.dbdesc['kwargs'])
		
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

	def pagingSQL(self,sql,paging,NS):
		"""
		default it not support paging
		"""
		page = int(NS.get(paging['pagename'],1))
		rows = int(NS.get(paging['rowsname'],10))
		sort = NS.get(paging.get('sortname','sort'),None)
		order = NS.get(paging.get('ordername','asc'),'asc')
		if not sort:
			return sql
		if page < 1:
			page = 1
		from_line = (page - 1) * rows
		end_line = page * rows + 1
		psql = self.pagingSQLmodel()
		ns={
			'from_line':from_line,
			'end_line':end_line,
			'rows':rows,
			'sort':sort,
			'order':order,
		}
		ac = ArgsConvert('$[',']$')
		psql = ac.convert(psql,ns)
		retSQL=psql % sql
		return retSQL
		
	def pagingSQLmodel(self):
		return u"""select * from (%s) A order by $[sort]$ $[order]$
limit $[from_line]$,$[rows]$"""

	def tablesSQL(self):
		sqlcmd = """SELECT lower(TABLE_NAME) as name, lower(TABLE_COMMENT) as title FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '%s'""" % self.dbdesc.get('dbname','unknown')
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
 from information_schema.columns where lower(TABLE_SCHEMA) = '%s' """ % self.dbdesc.get('dbname','unknown').lower()
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
""" % self.dbdesc.get('dbname','unknown').lower()
		if tablename is not None:
			sqlcmd = sqlcmd + " and C.REFERENCED_TABLE_NAME = '%s'" % tablename.lower()
		return sqlcmd

	def pkSQL(self,tablename=None):
		sqlcmd = """SELECT column_name as name FROM INFORMATION_SCHEMA.`KEY_COLUMN_USAGE` WHERE table_name='%s' AND constraint_name='PRIMARY'
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
    table_schema = '%s'""" % self.dbdesc.get('dbname','unknown')
		if tablename is not None:
			sqlcmd = sqlcmd + """	AND table_name = '%s'""" % tablename.lower()
		return sqlcmd
