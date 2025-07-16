from .sor import SQLor
from .ddl_template_oracle import oracle_ddl_tmpl
class Oracleor(SQLor):
	ddl_template = oracle_ddl_tmpl
	db2modelTypeMapping = {
		'char':'char',
		'nchar':'str',
		'varchar':'str',
		'varchar2':'str',
		'nvarchar2':'str',
		'number':'long',
		'integer':'long',
		'binary_float':'float',
		'binary_double':'float',
		'float':'float',
		'timestamp':'timestamp',
		'timestamp with time zone':'timestamp',
		'timestamp with local time zone':'timestamp',
		'interval year to moth':'date',
		'interval day to second':'timestamp',
		'clob':'text',
		'nclob':'text',
		'blob':'file',
		'bfile':'file',
		'date':'date',
	}
	model2dbTypemapping = {
		'date':'date',
		'time':'date',
		'timestamp':'date',
		'str':'varchar2',
		'char':'char',
		'short':'number',
		'long':'number',
		'float':'number',
		'text':'nclob',
		'file':'blob',
	}
	@classmethod
	def isMe(self,name):
		return name=='cx_Oracle'

	def grammar(self):
		return {
			'select':select_stmt,
		}
		
	def placeHolder(self,varname,pos=None):
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
	order by $[sort]$
	) 
where row_id >=$[from_line]$ and row_id < $[end_line]$"""

	def tablesSQL(self):
		sqlcmd = """select 
lower(table_name) as name,
lower(decode(comments,null,table_name,comments)) as title
from USER_TAB_COMMENTS where table_type = 'TABLE'"""
		return sqlcmd
	
	def fieldsSQL(self,tablename=None):
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
		sqlcmd = """select 
  lower(a.index_name) index_name,
  lower(a.UNIQUENESS) is_unique,
  lower(b.column_name) column_name 
from user_indexes a, user_ind_columns b
where a.index_name = b.index_name"""
		if tablename is not None:
			sqlcmd += """  and lower(a.table_name) = lower('%s')"""  % tablename.lower()
		return sqlcmd
		
