import os  
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
import sys
import codecs
import re
import json
from appPublic.myImport import myImport
from appPublic.dictObject import DictObject,dictObjectFactory
from appPublic.unicoding import uDict
from appPublic.myTE import MyTemplateEngine


from appPublic.argsConvert import ArgsConvert,ConditionConvert

class SQLorException(Exception,object):
	def __int__(self,**kvs):
		supper(SQLException,self).__init__(self,**kvs)
		self.dic = {
			'response':'error',
			'errtype':'SQLor',
			'errmsg':supper(SQLException,self).message,
		}
		
	def __str__(self):
		return 'errtype:%s,errmsg=%s' % (self.dic['errtype'],self.dic['errmsg'])
	
def setValues(params,ns):
	r = ns.get(params,os.getenv(params))
	return r
		
def findNamedParameters(sql):
	"""
	return  a list of named parameters
	"""
	re1 = '\$\{[_a-zA-Z_][a-zA-Z_0-9]*\}'
	params1 = re.findall(re1,sql)
	return params1


def uniParams(params1):
	ret = []
	for i in params1:
		if i not in ret:
			ret.append(i)
	return ret

def readsql(fn):
	f = codecs.open(fn,'r','utf-8')
	b = f.read()
	f.close()
	return b

class SQLor(object):
	def __init__(self,dbdesc=None,sqltp = '$[',sqlts = ']$',sqlvp = '${',sqlvs = '}$'):
		self.conn = None
		self.cur = None
		self.async_mode = False
		self.sqltp = sqltp
		self.sqlts = sqlts
		self.sqlvp = sqlvp
		self.sqlvs = sqlvs
		self.dbdesc = dbdesc
		self.writer = None
		self.convfuncs = {}
		self.cc = ConditionConvert()
	
	def setCursor(self,async_mode,conn,cur):
		self.async_mode = async_mode
		self.conn = conn
		self.cur = cur

	def getConn(self):
		return self.conn
	
	def setConvertFunction(self,typ,func):
		self.convfuncs.update({typ:func})
	
	def convert(self,typ,value):
		if self.convfuncs.get(typ,None) is not None:
			return self.convfuncs[typ](value)
		return value
	@classmethod
	def isMe(self,name):
		return name=='sqlor'
		
	def pagingSQLmodel(self):
		return u""
		
	def placeHolder(self,varname):
		if varname=='__mainsql__' :
			return ''
		return '?'
	
	def dataConvert(self,dataList):
		return [ i.get('value',None) for i in dataList]
	
	def dataList(self,k,v):
		a = []
		a.append({'name':k,'value':v})
		return a
		
	def cursor(self):
		return self.cur
	
	def recordCnt(self,sql):
		ret = u"""select count(*) rcnt from (%s) rowcount_table""" % sql
		return ret
	
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
		from_line = (page - 1) * rows + 1
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
	
	def filterSQL(self,sql,filters,NS):
		ac = ArgsConvert('$[',']$')
		fbs = []
		for f in filters:
			vars = ac.findAllVariables(f)
			if len(vars) > 0:
				ignoreIt = False
				for v in vars:
					if not NS.get(v,False):
						ignoreIt = True
				if not ignoreIt:
					f = ac.convert(f,NS)
				else:
					f = '1=1'
			fbs.append(f)
		fb = ' '.join(fbs)
		retsql = u"""select * from (%s) filter_table where %s""" % (sql,fb)
		return retsql
		
	async def runVarSQL(self,cursor,sql,NS):
		"""
		using a opened cursor to run a SQL statment with variable, the variable is setup in NS namespace
		return a cursor with data
		"""					
		markedSQL,datas = self.maskingSQL(sql,NS)
		datas = self.dataConvert(datas)
		try:
			# markedSQL = markedSQL.encode('utf8')
			if self.async_mode:
				await cursor.execute(markedSQL,datas)
			else:
				cursor.execute(markedSQL,datas)

		except Exception as e:
			print( "markedSQL=",markedSQL,':',datas,':',e)
			raise e
		return 
			
	def maskingSQL(self,org_sql,NS):
		"""
		replace all ${X}$ format variable exception named by '__mainsql__' in sql with '%s', 
		and return the marked sql sentent and variable list
		sql is a sql statment with variable formated in '${X}$
		the '__mainsql__' variable use to identify the main sql will outout data.
		NS is the name space the variable looking for, it is a variable dictionary 
		return (MarkedSQL,list_of_variable)
		"""
		sqltextAC = ArgsConvert(self.sqltp,self.sqlts)
		sqlargsAC = ArgsConvert(self.sqlvp,self.sqlvs)
		sql1 = sqltextAC.convert(org_sql,NS)
		cc = ConditionConvert()
		sql1 = cc.convert(sql1,NS)
		vars = sqlargsAC.findAllVariables(sql1)
		phnamespace = {}
		[phnamespace.update({v:self.placeHolder(v)}) for v in vars]
		m_sql = sqlargsAC.convert(sql1,phnamespace)
		newdata = []
		for v in vars:
			if v != '__mainsql__':
				value = sqlargsAC.getVarValue(v,NS,None)
				newdata += self.dataList(v,value)
		
		return (m_sql,newdata)
		
	async def execute(self,sql,value,callback,**kwargs):
		cur = self.cursor()
		await self.runVarSQL(cur,sql,value)
		if callback is not None:
			fields = [ i[0].lower() for i in cur.description ]
			rec = None
			if self.async_mode:
				rec = await cur.fetchone()
			else:
				rec = cur.fetchone()

			while rec is not None:
				dic = {}
				for i in range(len(fields)):
					dic.update({fields[i]:rec[i]})
				callback(DictObject(**dic),**kwargs)
				if self.async_mode:
					rec = await cur.fetchone()
				else:
					rec = cur.fetchone()
		

	async def executemany(self,sql,values):
		cur = self.cursor()
		markedSQL,datas = self.maskingSQL(sql,{})
		datas = [ self.dataConvert(d) for d in values ]
		if async_mode:
			await cur.executemany(markedSQL,datas)
		else:
			cur.executemany(markedSQL,datas)
	
	def pivotSQL(self,tablename,rowFields,columnFields,valueFields):
		def maxValue(columnFields,valueFields,cfvalues):
			sql = ''
			for f in valueFields:
				i = 0			
				for field in columnFields:
					for v in cfvalues[field]:
						sql += """
		,sum(%s_%d) %s_%d""" % (f,i,f,i)
						i+=1
			return sql
		def casewhen(columnFields,valueFields,cfvalues):
			sql = ''
			for f in valueFields:
				i = 0			
				for field in columnFields:
					for v in cfvalues[field]:
						if v is None:
							sql += """,case when %s is null then %s
			else 0 end as %s_%d  -- %s
		""" % (field,f,f,i,v)
						else:
							sql += """,case when trim(%s) = trim('%s') then %s
			else 0 end as %s_%d  -- %s
		""" % (field,v,f,f,i,v)
						
						i += 1
			return sql
	
		cfvalues={}
		for field in columnFields:
			sqlstring = 'select distinct %s from %s' % (field,tablename)
			v = []
			self.execute(sqlstring,{},lambda x: v.append(x))
			cfvalues[field] = [ i[field] for i in v ]
		
		sql ="""
	select """ + ','.join(rowFields)
		sql += maxValue(columnFields,valueFields,cfvalues)
		sql += """ from 
	(select """  + ','.join(rowFields)
		sql += casewhen(columnFields,valueFields,cfvalues)
		sql += """
	from %s)
	group by %s""" % (tablename,','.join(rowFields))
		return sql
		
	async def pivot(self,desc,tablename,rowFields,columnFields,valueFields):
		sql = self.pivotSQL(tablename,rowFields,columnFields,valueFields)
		desc['sql_string'] = sql
		ret = []
		return await self.execute(sql,{},lambda x:ret.append(x))

	def isSelectSql(self,sql):
		i = 0
		while sql[i] in "\r\n \t":
			i = i + 1
		return sql.lower().startswith('select ')

	def getSQLfromDesc(self,desc):
		sql = ''
		if 'sql_file' in desc.keys():
			sql = readsql(desc['sql_file'])
		else:
			sql = desc['sql_string']
		return sql
		
	async def record_count(self,desc,NS):
		cnt_desc = {}
		cnt_desc.update(desc)
		sql = self.getSQLfromDesc(desc)
		if desc.get('sql_file',False):
			del cnt_desc['sql_file']
		cnt_desc['sql_string'] = self.recordCnt(sql)
		class Cnt:
			def __init__(self):
				self.recs = []
			def handler(self,rec):
				self.recs.append(rec)

		c = Cnt()
		await self.runSQL(cnt_desc,NS,c.handler)
		t = c.recs[0]['rcnt']
		return t

	async def pagingdata(self,desc,NS):
		paging_desc = {}
		paging_desc.update(desc)
		paging_desc.update(
			{
				"paging":{
					"rowsname":"rows",
					"pagename":"page",
					"sortname":"sort",
					"ordername":"order"
				}
			})
		if desc.get('sortfield',False):
			NS['sort'] = desc.get('sortfield')
		sql = self.getSQLfromDesc(desc)
		if desc.get('sql_file',False):
			del cnt_desc['sql_file']
		paging_desc['sql_string'] = self.pagingSQL(sql,
					paging_desc.get('paging'),NS)

		class Cnt:
			def __init__(self):
				self.recs = []
			def handler(self,rec):
				self.recs.append(rec)

		c = Cnt()
		await self.runSQL(paging_desc,NS,c.handler)
		return c.recs

	async def resultFields(self,desc,NS):
		NS.update(rows=1,page=1)
		r = await self.pagingdata(desc,NS)
		ret = [ DictObject(**{'name':i[0],'type':i[1]}) for i in self.cur.description ]
		return ret
		
	async def runSQL(self,desc,NS,callback,**kw):
		class RecordHandler:
			def __init__(self,ns,name):
				self.ns = ns
				self.name = name
				self.ns[name] = []

			def handler(self,rec):
				obj = DictObject(**rec)
				self.ns[self.name].append(obj)

		cur = self.cursor()
		sql = self.getSQLfromDesc(desc)
		if self.isSelectSql(sql):
			if callback is None:
				klass = desc.get('dataname','dummy')
				if klass is not None:
					rh = RecordHandler(NS,klass)
					callback = rh.handler
		else:
			callback = None
		await self.execute(sql,NS,callback)
	
	async def sqlExecute(self,desc,NS):
		await self.execute(sql,NS,None)
	
	async def tables(self):
		sqlstring = self.tablesSQL()
		ret = []
		await self.execute(sqlstring,{},lambda x:ret.append(x))
		return ret
	
	def indexesSQL(self,tablename):
		"""
		record of {
			index_name,
			index_type,
			table_name,
			column_name
		}
		"""
		return None
		
	async def indexes(self,tablename=None):
		sqlstring = self.indexesSQL(tablename.lower())
		if sqlstring is None:
			return []
		recs = []
		await self.execute(sqlstring,{},lambda x:recs.append(x))
		return recs
		
	async def fields(self,tablename=None):
		sqlstring = self.fieldsSQL(tablename)
		recs = []
		await self.execute(sqlstring,{},lambda x:recs.append(x))
		ret = []
		for r in recs:
			r.update({'type':self.db2modelTypeMapping.get(r['type'].lower(),'unknown')})
			r.update({'name':r['name'].lower()})
			ret.append(r)
		return ret
	
	async def primary(self,tablename):
		sqlstring = self.pkSQL(tablename)
		recs = []
		await self.execute(sqlstring,{},lambda x:recs.append(x))
		return recs
		
	async def fkeys(self,tablename):
		sqlstring = self.fkSQL(tablename)
		recs = []
		await self.execute(sqlstring,{},lambda x:recs.append(x))
		return recs
	
	async def createTable(self,tabledesc):
		te = MyTemplateEngine([],'utf8','utf8')
		desc = {
			"sql_string":te.renders(self.ddl_template,tabledesc)
		}
		return await self.sqlExecute(desc,{})
		
	async def getTableDesc(self,tablename):
		desc = {}
		summary = [ i for i in await self.tables() if tablename.lower() == i.name ]
		primary = [i.field_name for i in await self.primary(tablename) ]
		summary['primary'] = primary
		desc['summary'] = summary
		desc['fields'] = await self.fields(tablename=tablename)
		desc['validation'] = []
		idx = {}
		async for idxrec in self.indexes(tablename=tablename):
			if idxrec.index_name != idx.get('name',None):
				if idx != {}:
					desc['validation'].append(idx)
					idx = {
						'fields':[]
					}
				else:
					idx['fields'] = []
				idx['name'] = idxrec.index_name
				idx['oper'] = 'idx'
			idx['fields'].append(idxrec.field_name)
		if idx != {}:
			desc['validation'].append(idx)		
		return desc
	
		
				
