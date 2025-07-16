import re
from .sor import SQLor

class SQLite3or(SQLor):
	db2modelTypeMapping = {
		'text':'str',
		'blob':'file',
		'int':'long',
		'integer':'long',
		'real':'float',
	}
	model2dbTypemapping = {
		'date':'text',
		'time':'text',
		'timestamp':'text',
		'str':'text',
		'char':'text',
		'short':'int',
		'long':'int',
		'float':'real',
		'text':'text',
		'file':'blob',
	}
	@classmethod
	def isMe(self,name):
		return name=='sqlite3'
			
	def placeHolder(self,varname,pos=None):
		if varname=='__mainsql__' :
			return ''
		return '?'
	
	def dataConvert(self,dataList):
		if type(dataList) == type({}):
			d = [ i for i in dataList.values()]
		else:
			d = [ i['value'] for i in dataList]
		return tuple(d)

	def pagingSQLmodel(self):
		sql = u"""select * from (%s) order by $[sort]$ limit $[from_line]$,$[end_line]$"""
		return sql

	def tablesSQL(self):
		sqlcmd = u"""select name, tbl_name as title from sqlite_master where upper(type) = 'TABLE'"""
		return sqlcmd
	
	def fieldsSQL(self,tablename):
		# sqlcmd="""PRAGMA table_info('%s')""" % tablename.lower()
		return sqlcmd

	def fields(self,tablename):
		m = u'(\w+)\(((\d+)(,(\d+)){0,1})\){0,1}'
		k = re.compile(m)
		def typesplit(typ):
			d = k.search(typ)
			if d is None:
				return typ,0,0
				
			return d.group(1),int(d.group(3) if d.group(3) is not None else 0 ),int(d.group(5) if d.group(5) is not None else 0)
			
		sqlstring = self.fieldsSQL(tablename)
		recs = []
		self.execute(sqlstring,callback=lambda x:recs.append(x))
		for r in recs:
			t,l,d = typesplit(r['type'])
			r['type'] = t
			r['length'] = int(l)
			r['dec'] = int(d)
			r['title'] = r['name']
		ret = []
		for r in recs:
			r.update({'type':self.db2modelTypeMapping.get(r['type'].lower(),'text')})
			r.update({'name':r['name'].lower()})
			ret.append(r)
		return ret
		
	def fkSQL(self,tablename):
		sqlcmd = ""
		return sqlcmd
		
	def fkeys(self,tablename):
		return []
		
	def primary(self,tablename):
		recs = self.fields(tablename)
		ret = [ {'field':r['name']} for r in recs if r['pk'] == 1 ]
		return ret
		
	def pkSQL(self,tablename):
		sqlcmd = ""
		return sqlcmd

	def indexesSQL(self,tablename=None):
		sqlcmd = """select * from sqlite_master 
where lower(type) = 'index'
	"""
		if tablename:
			sqlcmd += "and lower(tbl_name)='" + tablename.lower() + "' "
		return sqlcmd
