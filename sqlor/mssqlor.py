# -*- coding:utf8 -*-
from .sor import SQLor
from .ddl_template_sqlserver import sqlserver_ddl_tmpl

class MsSqlor(SQLor):
	ddl_template = sqlserver_ddl_tmpl
	db2modelTypeMapping = {
		'bit':'short',
		'tinyint':'short',
		'date':'date',
		'bigint':'long',
		'smallint':'short',
		'int':'long',
		'decimal':'float',
		'numeric':'float',
		'smallmoney':'float',
		'money':'float',
		'real':'float',
		'float':'float',
		'datetime':'date',
		'timestamp':'timestamp',
		'uniqueidentifier':'timestamp',
		'char':'char',
		'varchar':'str',
		'text':'text',
		'nchar':'str',
		'nvarchar':'str',
		'ntext':'text',
		'binary':'str',
		'varbinary':'str',
		'image':'file',
	}
	model2dbTypemapping = {
		'date':'datetime',
		'time':'date',
		'timestamp':'timestamp',
		'str':'nvarchar',
		'char':'char',
		'short':'int',
		'long':'numeric',
		'float':'numeric',
		'text':'ntext',
		'file':'image',
	}
	@classmethod
	def isMe(self,name):
		return name=='pymssql'
			
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
		return u"""select *
from (
	select row_number() over(order by $[sort]$) as _row_id,page_s.* 
	from (%s) page_s
	) A
where _row_id >= $[from_line]$ and _row_id < $[end_line]$"""

	def tablesSQL(self):
		sqlcmd = u"""select 
	lower(d.name) as name,
	lower(cast(Isnull(f.VALUE,d.name) as nvarchar )) title
	from sysobjects d  
		left join sys.extended_properties f on d.id = f.major_id and f.minor_id = 0
	where d.xtype = 'U'"""
		return sqlcmd
	
	def fieldsSQL(self,tablename=None):
		sqlcmd=u"""SELECT name = lower(a.name)
		   ,type = b.name
		   ,length = Columnproperty(a.id,a.name,'PRECISION')
		   ,dec = Isnull(Columnproperty(a.id,a.name,'Scale'),null)
		   ,nullable = CASE 
					WHEN a.isnullable = 1 THEN 'yes'
					ELSE 'no'
				  END
		   ,title = lower(cast(Isnull(g.[value],a.name) as nvarchar) )
		   ,table_name = lower(d.name)
	FROM     syscolumns a
			 LEFT JOIN systypes b
			   ON a.xusertype = b.xusertype
			 INNER JOIN sysobjects d
			   ON (a.id = d.id)
				  AND (d.xtype = 'U')
				  AND (d.name <> 'dtproperties') 
			  INNER JOIN  sys.all_objects c
				ON d.id=c.object_id 
					AND  schema_name(schema_id)='dbo'
			 LEFT JOIN sys.extended_properties g
			   ON (a.id = g.major_id)
				  AND (a.colid = g.minor_id)
			 LEFT JOIN sys.extended_properties f
			   ON (d.id = f.major_id)
				  AND (f.minor_id = 0)"""
		if tablename is not None:
			sqlcmd = sqlcmd + """ where lower(d.name)='%s'
	ORDER BY a.id,a.colorder""" % tablename.lower()
		else:
			sqlcmd = sqlcmd + """ ORDER BY a.id,a.colorder"""
		return sqlcmd

	def fkSQL(self,tablename=None):
		sqlcmd = u"""select
  MainCol.name AS field          -- [主表列名]
  ,oSub.name  AS  fk_table       -- [子表名称],
  ,SubCol.name AS fk_field       -- [子表列名],
from
  sys.foreign_keys fk  
    JOIN sys.all_objects oSub  
        ON (fk.parent_object_id = oSub.object_id)
    JOIN sys.all_objects oMain 
        ON (fk.referenced_object_id = oMain.object_id)
    JOIN sys.foreign_key_columns fkCols 
        ON (fk.object_id = fkCols.constraint_object_id)
    JOIN sys.columns SubCol 
        ON (oSub.object_id = SubCol.object_id  
            AND fkCols.parent_column_id = SubCol.column_id)
    JOIN sys.columns MainCol 
        ON (oMain.object_id = MainCol.object_id  
            AND fkCols.referenced_column_id = MainCol.column_id)"""
		if tablename is not None:
			sqlcmd = sqlcmd + """	where lower(oMain.name) = '%s'""" % tablename.lower()

		return sqlcmd

	def pkSQL(self,tablename=None):
		sqlcmd = u"""select 
	lower(a.table_name) as table_name,
	lower(b.column_name) as field_name
 from information_schema.table_constraints a
 inner join information_schema.constraint_column_usage b
 on a.constraint_name = b.constraint_name
 where a.constraint_type = 'PRIMARY KEY'"""
		if tablename is not None:
			sqlcmd = sqlcmd + """ and lower(a.table_name) = '%s'""" % tablename.lower()
		return sqlcmd

	def indexesSQL(self,tablename=None):
		sqlcmd = """SELECT
index_name=lower(IDX.Name),
index_type=IDX.is_unique,
column_name=lower(C.Name)
FROM sys.indexes IDX 
INNER JOIN sys.index_columns IDXC
ON IDX.[object_id]=IDXC.[object_id]
AND IDX.index_id=IDXC.index_id
LEFT JOIN sys.key_constraints KC
ON IDX.[object_id]=KC.[parent_object_id]
AND IDX.index_id=KC.unique_index_id
INNER JOIN sys.objects O
ON O.[object_id]=IDX.[object_id]
INNER JOIN sys.columns C
ON O.[object_id]=C.[object_id]
AND O.type='U'
AND O.is_ms_shipped=0
AND IDXC.Column_id=C.Column_id"""
		if tablename is not None:
			sqlcmd = sqlcmd + """ where lower(O.name)='%s'""" % tablename.lower()
		return sqlcmd
