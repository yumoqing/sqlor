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
		return name=='psycopg2' or name=='pyguass'

	def grammar(self):
		return {
			'select':select_stmt,
		}
		
	def placeHolder(self,varname,i):
		if varname=='__mainsql__' :
			return ''
		return '%%(%s)s' % varname
	
	def dataConvert(self,dataList):
		if type(dataList) == type({}):
			return dataList
		d = { i['name']:i['value'] for i in dataList }
		return d

	def pagingSQLmodel(self):
		return u"""select * 
from (
	select page_s.*,rownum row_id 
	from (%s) page_s 
	order by $[sort]$
	) 
where row_id >=$[from_line]$ and row_id < $[end_line]$"""

	def tablesSQL(self):
		sqlcmd = """select x.name, y.description as title
from 
(select a.name,c.oid
	from
	(select lower(tablename) as name from pg_tables where schemaname='public') a,
	pg_class c
where a.name = c.relname) x
left join pg_description y
on x.oid=y.objoid
	and y.objsubid='0'"""
		return sqlcmd
	
	def fieldsSQL(self,tablename=None):
		sqlcmd="""SELECT 
	a.attname AS name, 
	t.typname AS type, 
	case t.typname
		when 'varchar' then  a.atttypmod - 4
		when 'numeric' then (a.atttypmod - 4) / 65536
		else null
	end as length,
	case t.typname
		when 'numeric' then (a.atttypmod - 4) %% 65536
		else null
	end as dec,
    case a.attnotnull
		when 't' then 'no'
		when 'f' then 'yes'
	end as nullable,
	b.description AS title
FROM pg_class c, pg_attribute a
    LEFT JOIN pg_description b
    ON a.attrelid = b.objoid
        AND a.attnum = b.objsubid, pg_type t
WHERE lower(c.relname) = '%s'
    AND a.attnum > 0
    AND a.attrelid = c.oid
    AND a.atttypid = t.oid
ORDER BY a.attnum;
		""" % tablename.lower()
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
		sqlcmd="""
		select 
	pg_attribute.attname as field_name,
	lower(pg_class.relname) as table_name
from pg_constraint 
	inner join pg_class 
		on pg_constraint.conrelid = pg_class.oid 
	inner join pg_attribute 
		on pg_attribute.attrelid = pg_class.oid 
			and  pg_attribute.attnum = pg_constraint.conkey[1]
	inner join pg_type 
		on pg_type.oid = pg_attribute.atttypid
where lower(pg_class.relname) = '%s' 
and pg_constraint.contype='p'
		""" % tablename.lower()
		return sqlcmd
		
	def indexesSQL(self,tablename=None):
		sqlcmd = """select
    i.relname as index_name,
	case ix.INDISUNIQUE
		when 't' then 'unique'
		else ''
	end as is_unique,
    a.attname as column_name
from
    pg_class t,
    pg_class i,
    pg_index ix,
    pg_attribute a
where
    t.oid = ix.indrelid
    and i.oid = ix.indexrelid
    and a.attrelid = t.oid
    and a.attnum = ANY(ix.indkey)
    and t.relkind = 'r'
    and lower(t.relname) = '%s'
order by
    t.relname,
    i.relname""" % tablename.lower()
		return sqlcmd
		
