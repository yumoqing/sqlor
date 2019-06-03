from .sor import SQLor
from .ddl_template_postgresql import postgresql_ddl_tmpl

class PostgreSQLor(SQLor):
	ddl_template = postgresql_ddl_tmpl
	db2modelTypeMapping = {
		'smallint':'short',
		'integer':'long',
		'bigint':'llong',
		'decimal':'float',
		'numeric':'float',
		'real':'float',
		'double':'float',
		'serial':'long',
		'bigserial':'llong',
		'char':'char',
		'character':'char',
		'varchar':'str',
		'character varying':'str',
		'text':'text',
		'timestamp':'timestamp',
		'date':'date',
		'time':'time',
		'boolean':'char',
		'bytea':'file'
	}
	model2dbTypemapping = {
		'date':'date',
		'time':'date',
		'timestamp':'timestamp',
		'str':'varchar',
		'char':'char',
		'short':'smallint',
		'long':'integer',
		'float':'numeric',
		'text':'text',
		'file':'bytea',
	}
	@classmethod
	def isMe(self,name):
		return name=='psycopg2'

	def grammar(self):
		return {
			'select':select_stmt,
		}
		
	def placeHolder(self,varname):
		if varname=='__mainsql__' :
			return ''
		return ':%s' % varname
	
	def dataConvert(self,dataList):
		if type(dataList) == type({}):
			return dataList
		d = {}
		[ d.update({i['name']:i['value']}) for i in dataList ]
		return d

	def pagingSQLmodel(self):
		return u"""select * 
from (
	select page_s.*,rownum row_id 
	from (%s) page_s 
	order by $[sort]$ $[order]$
	) 
where row_id >=$[from_line]$ and row_id < $[end_line]$"""

	def tablesSQL(self):
		"""
			列出表名
			SELECT   tablename   FROM   pg_tables;
			WHERE   tablename   NOT   LIKE   'pg%'
			AND tablename NOT LIKE 'sql_%' 
			ORDER   BY   tablename;
		"""
		sqlcmd = """select 
lower(table_name) as name,
lower(decode(comments,null,table_name,comments)) as title
from USER_TAB_COMMENTS where table_type = 'TABLE'"""
		return sqlcmd
	
	def fieldsSQL(self,tablename=None):
		"""SELECT col_description(a.attrelid,a.attnum) as comment,pg_type.typname as typename,a.attname as name, a.attnotnull as notnull
FROM pg_class as c,pg_attribute as a inner join pg_type on pg_type.oid = a.atttypid
where c.relname = 'tablename' and a.attrelid = c.oid and a.attnum>0
		"""
		sqlcmd="""select lower(utc.COLUMN_NAME) name
	,utc.DATA_TYPE type
	,utc.DATA_LENGTH length
	,utc.data_scale dec
	,case when utc.nullable = 'Y' then 'yes' else 'no' end nullable
	,lower(nvl(ucc.comments,utc.COLUMN_NAME)) title
	,lower(utc.table_name) as table_name
	from  user_tab_cols utc left join USER_COL_COMMENTS ucc on utc.table_name = ucc.table_name and utc.COLUMN_NAME = ucc.COLUMN_NAME"""
		if tablename is not None:
			sqlcmd = sqlcmd + """ where lower(utc.table_name) = '%s'""" % tablename.lower()
		return sqlcmd
	
	def fkSQL(self,tablename=None):
		tablename = tablename.lower()
		sqlcmd = """select
 distinct(ucc.column_name) as field,rela.table_name as fk_table,rela.column_name as fk_field
from
 user_constraints uc,user_cons_columns ucc,
 (
	select t2.table_name,t2.column_name,t1.r_constraint_name 
		from user_constraints t1,user_cons_columns t2 
		where t1.r_constraint_name=t2.constraint_name 
) rela
where
 uc.constraint_name=ucc.constraint_name
 and uc.r_constraint_name=rela.r_constraint_name"""
		if tablename is not None:
			sqlcmd = sqlcmd + """ and lower(uc.table_name)='%s'""" % tablename.lower()
		return sqlcmd
	
	def pkSQL(self,tablename=None):
		"""
		select pg_attribute.attname as colname,pg_type.typname as typename,pg_constraint.conname as pk_name from 
pg_constraint  inner join pg_class 
on pg_constraint.conrelid = pg_class.oid 
inner join pg_attribute on pg_attribute.attrelid = pg_class.oid 
and  pg_attribute.attnum = pg_constraint.conkey[1]
inner join pg_type on pg_type.oid = pg_attribute.atttypid
where pg_class.relname = 'tablename' 
and pg_constraint.contype='p'
		"""
		sqlcmd = """
select
 lower(col.table_name) table_name,
 lower(col.column_name) as field_name
from
 user_constraints con,user_cons_columns col
where
 con.constraint_name=col.constraint_name and con.constraint_type='P'"""
		if tablename is not None:
			sqlcmd = sqlcmd + """ and lower(col.table_name)='%s'""" % tablename.lower()
		return sqlcmd
		
	def indexesSQL(self,tablename=None):
		"""
SELECT

A.SCHEMANAME,

A.TABLENAME,

A.INDEXNAME,

A.TABLESPACE,

A.INDEXDEF,

B.AMNAME,

C.INDEXRELID,

C.INDNATTS,

C.INDISUNIQUE,

C.INDISPRIMARY,

C.INDISCLUSTERED,

D.DESCRIPTION

FROM

PG_AM B

LEFT JOIN PG_CLASS F ON B.OID = F.RELAM

LEFT JOIN PG_STAT_ALL_INDEXES E ON F.OID = E.INDEXRELID

LEFT JOIN PG_INDEX C ON E.INDEXRELID = C.INDEXRELID

LEFT OUTER JOIN PG_DESCRIPTION D ON C.INDEXRELID = D.OBJOID,

PG_INDEXES A

WHERE

A.SCHEMANAME = E.SCHEMANAME AND A.TABLENAME = E.RELNAME AND A.INDEXNAME = E.INDEXRELNAME

AND E.SCHEMANAME = 'public' AND E.RELNAME = 'table_name'
"""
		sqlcmd = """select 
  lower(a.index_name) index_name,
  lower(a.UNIQUENESS) index_type,
  lower(a.table_name) table_name,
  lower(b.column_name) column_name 
from user_indexes a, user_ind_columns b
where a.index_name = b.index_name"""
		if tablename is not None:
			sqlcmd += """  and lower(a.table_name) = lower('%s')"""  % tablename.lower()
		return sqlcmd
		
