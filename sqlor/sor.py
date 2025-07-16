from traceback import format_exc
import os  
import decimal
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
import sys
from datetime import datetime, date
import codecs
import re
import json
from appPublic.myImport import myImport
from appPublic.dictObject import DictObject
from appPublic.unicoding import uDict
from appPublic.myTE import MyTemplateEngine
from appPublic.objectAction import ObjectAction
from appPublic.argsConvert import ArgsConvert,ConditionConvert
from appPublic.registerfunction import RegisterFunction
from appPublic.log import info, exception, debug
from .filter import DBFilter

def db_type_2_py_type(o):
	if isinstance(o,decimal.Decimal):
		return float(o)
	if isinstance(o,datetime):
		# return '%020d' % int(o.timestamp() * 1000)
		return str(o)
	if isinstance(o, date):
		return '%04d-%02d-%02d' % (o.year, o.month, o.day)
	return o

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
		self.dbname = self.dbdesc.get('dbname')
		if self.dbname:
			self.dbname = self.dbname.lower()
		self.writer = None
		self.convfuncs = {}
		self.cc = ConditionConvert()
		self.dataChanged = False
		self.metadatas={}

	async def get_schema(self):
		def concat_idx_info(idxs):
			x = []
			n = None
			for i in idxs:
				if not n or n.index_name != i.index_name:
					if n:
						x.append(n)
					n = i
					n.column_name = [i.column_name]
				else:
					n.column_name.append(i.column_name)
			return x

		tabs = await self.tables()
		schemas = []
		for t in tabs:
			primary = await self.primary(t.name)
			# print('primary=', primary)
			indexes = concat_idx_info(await self.indexes(t.name))
			fields = await self.fields(t.name)
			primary_fields = [f.field_name for f in primary]
			if len(primary_fields)>0:
				t.primary = [f.field_name for f in primary]
			x = {}
			x['summary'] = [t]
			x['indexes'] = indexes
			x['fields'] = fields
			schemas.append(x)
		return schemas

	def setMeta(self,tablename,meta):
		self.metadatas[tablename.lower()] = meta

	def getMeta(self,tablename):
		return self.metadatas.get(tablename.lower(),None)

	def removeMeta(self,tablename):
		if getMeta(self.tablename):
			del self.metadatas[tablename.lower()]

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
		return ""
		
	def placeHolder(self,varname,pos=None):
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
	
	def sortSQL(self, sql, NS):
		sort = NS.get('sort',None)
		if sort is None:
			return sql
		if isinstance(sort, list):
			sort = ','.join(sort)
		return sql + ' ORDER BY ' + sort

	def pagingSQL(self,sql,paging,NS):
		"""
		default it not support paging
		"""
		page = int(NS.get(paging['pagename'],1))
		rows = int(NS.get(paging['rowsname'],80))
		sort = NS.get(paging.get('sortname','sort'),None)
		if isinstance(sort, list):
			sort = ','.join(sort)
		if not sort:
			return sql
		if page < 1:
			page = 1
		from_line = (page - 1) * rows
		end_line = page * rows
		psql = self.pagingSQLmodel()
		ns={
			'from_line':from_line,
			'end_line':end_line,
			'rows':rows,
			'sort':sort
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
		markedSQL, datas = self.maskingSQL(sql,NS)
		datas = self.dataConvert(datas)
		try:
			if self.async_mode:
				return await cursor.execute(markedSQL,datas)
			else:
				return cursor.execute(markedSQL,datas)

		except Exception as e:
			fe = format_exc()
			exception(f"{markedSQL=},{datas=}, {e=}, {fe=}")
			raise e
			
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
		[phnamespace.update({v:self.placeHolder(v,i)}) for i,v in enumerate(vars)]
		m_sql = sqlargsAC.convert(sql1,phnamespace)
		newdata = []
		for v in vars:
			if v != '__mainsql__':
				value = sqlargsAC.getVarValue(v,NS,None)
				newdata += self.dataList(v,value)
		
		return (m_sql,newdata)
		
	def getSqlType(self,sql):
		"""
		return one of "qry", "dml" and "ddl"
		ddl change the database schema
		dml change the database data
		qry query data
		"""
		
		a = sql.lstrip(' \t\n\r')
		a = a.lower()
		al = a.split(' ')
		if al[0] == 'select':
			return 'qry'
		if al[0] in ['update','delete','insert']:
			return 'dml'
		return 'ddl'
		
	async def execute(self,sql,value,callback,**kwargs):
		sqltype = self.getSqlType(sql)
		cur = self.cursor()
		ret = await self.runVarSQL(cur,sql,value)
		if sqltype == 'qry' and callback is not None:
			fields = [ i[0].lower() for i in cur.description ]
			rec = None
			if self.async_mode:
				rec = await cur.fetchone()
			else:
				rec = cur.fetchone()

			while rec is not None:
				dic = {}
				for i in range(len(fields)):
					dic.update({fields[i] : db_type_2_py_type(rec[i])})
				dic = DictObject(**dic)
				callback(dic,**kwargs)
				if self.async_mode:
					rec = await cur.fetchone()
				else:
					rec = cur.fetchone()
		if sqltype == 'dml':
			self.dataChanged = True
			return ret

	async def executemany(self,sql,values):
		cur = self.cursor()
		markedSQL,datas = self.maskingSQL(sql,{})
		datas = [ self.dataConvert(d) for d in values ]
		if self.async_mode:
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
		return self.getSqlType(sql) == 'qry'

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

	async def runSQLPaging(self,desc,NS):
		total = await self.record_count(desc,NS)
		recs = await self.pagingdata(desc,NS)
		data = {
			"total":total,
			"rows":recs
		}
		return data
		
	async def pagingdata(self,desc,NS):
		paging_desc = {}
		paging_desc.update(desc)
		paging_desc.update(
			{
				"paging":{
					"rowsname":"rows",
					"pagename":"page",
					"sortname":"sort"
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
		ret = [ {'name':i[0],'type':i[1]} for i in self.cur.description ]
		return ret
		
	async def runSQL(self,desc,NS,callback,**kw):
		class RecordHandler:
			def __init__(self,ns,name):
				self.ns = ns
				self.name = name
				self.ns[name] = []

			def handler(self,rec):
				self.ns[self.name].append(rec)

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
		return await self.execute(desc,NS,None)
	
	async def sqlExe(self,sql,ns):
		ret = []
		r = await self.execute(sql,ns,
			callback=lambda x:ret.append(x))
		sqltype = self.getSqlType(sql)
		if sqltype == 'dml':
			return r

		return ret

	async def sqlPaging(self,sql,ns):
		ret = []
		dic = {
			"sql_string":sql
		}
		page = ns.get('page')
		if not page:
			ns['page'] = 1

		total = await self.record_count(dic,ns)
		rows = await self.pagingdata(dic,ns)
		return {
			'total':total,
			'rows':rows
		}

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
		# print('sql=', sqlstring, 'recs=', recs)
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
		desc = self.getMeta(tablename)
		if desc:
			return desc
		desc = {}
		summary = [ i for i in await self.tables() if tablename.lower() == i['name'].lower() ]
		pris = await self.primary(tablename)
		primary = [i['name'] for i in pris ]
		summary[0]['primary'] = primary
		desc['summary'] = summary
		desc['fields'] = await self.fields(tablename=tablename)
		desc['indexes'] = []
		idx = {}
		idxrecs = await self.indexes(tablename)
		for idxrec in idxrecs:
			if idxrec['index_name'] == 'primary':
				continue
			if idxrec['index_name'] != idx.get('name',None):
				if idx != {}:
					desc['indexes'].append(idx)
					idx = {
					}
				idx['name'] = idxrec['index_name']
				idx['idxtype'] = 'unique' if idxrec['is_unique'] else 'index'
				idx['idxfields'] = []
			idx['idxfields'].append(idxrec['column_name'])
		if idx != {}:
			desc['indexes'].append(idx)
		self.setMeta(tablename,desc)
		return desc
	
	async def rollback(self):
		if self.async_mode:
			await self.conn.rollback()
		else:
			self.conn.rollback()
		self.dataChanged = False

	async def commit(self):
		if self.async_mode:
			await self.conn.commit()
		else:
			self.conn.commit()
		self.datachanged = False

	async def I(self,tablename):
		return await self.getTableDesc(tablename)

	async def C(self,tablename,ns):
		desc = await self.I(tablename)
		keys = ns.keys()
		fields = [ i['name'] for i in desc['fields'] if i['name'] in keys ]
		fns = ','.join(fields)
		vfns = ','.join(['${%s}$' % n for n in fields ])
		sql = 'insert into %s.%s (%s) values (%s)' % (self.dbname, tablename,fns,vfns)
		rf = RegisterFunction()
		rfname = f'{self.dbname}:{tablename}:c:before'
		ret = await rf.exe(rfname, ns)
		if isinstance(ret, dict):
			ns.update(ret)
		r = await self.runSQL({'sql_string':sql},ns.copy(), None)
		await rf.exe(f'{self.dbname}:{tablename}:c:after', ns)
		return r

	async def R(self,tablename,ns,filters=None):
		desc = await self.I(tablename)
		sql = 'select * from  %s.%s' % (self.dbname, tablename.lower())
		if filters:
			dbf = DBFilter(filters)
			sub =  dbf.genFilterString(ns)
			if sub:
				sql = '%s where %s' % (sql, sub)

		else:
			fields = [ i['name'] for i in desc['fields'] ]
			c = [ '%s=${%s}$' % (k,k) for k in ns.keys() if k in fields ]
			if len(c) > 0:
				sql = '%s where %s' % (sql,' and '.join(c))

		if 'page' in ns.keys():
			if not 'sort' in ns.keys():
				ns['sort'] = desc['summary'][0]['primary'][0]
			dic = {
				"sql_string":sql
			}
			total = await self.record_count(dic,ns)
			rows = await self.pagingdata(dic,ns)
			return {
				'total':total,
				'rows':rows
			}
		else:
			if ns.get('sort'):
				sql = self.sortSQL(sql, ns)
			return await self.sqlExe(sql,ns)

	async def U(self,tablename,ns):
		desc = await self.I(tablename)
		fields = [ i['name'] for i in desc['fields']]
		condi = [ i for i in desc['summary'][0]['primary']]
		newData = [ i for i in ns.keys() if i not in condi and i in fields]
		c = [ '%s = ${%s}$' % (i,i) for i in condi ]
		u = [ '%s = ${%s}$' % (i,i) for i in newData ]
		c_str = ' and '.join(c)
		u_str = ','.join(u)
		sql = 'update %s.%s set %s where %s' % (self.dbname, tablename,
					u_str,c_str)
		rf = RegisterFunction()
		ret = await rf.exe(f'{self.dbname}:{tablename}:u:before',ns)
		if isinstance(ret, dict):
			ns.update(ret)
		r = await self.runSQL({'sql_string':sql},ns.copy() ,None)
		await rf.exe(f'{self.dbname}:{tablename}:u:after',ns)
		return r

	async def D(self,tablename,ns):
		desc = await self.I(tablename)
		fields = [ i['name'] for i in desc['fields']]
		condi = [ i for i in desc['summary'][0]['primary']]
		c = [ '%s = ${%s}$' % (i,i) for i in condi ]
		c_str = ' and '.join(c)
		sql = 'delete from %s.%s where %s' % (self.dbname, tablename,c_str)
		rf = RegisterFunction()
		ret = await rf.exe(f'{self.dbname}:{tablename}:d:before', ns)
		if isinstance(ret, dict):
			ns.update(ret)
		r = await self.runSQL({'sql_string':sql},ns,None)
		ns = await rf.exe(f'{self.dbname}:{tablename}:d:after', ns)
		return r

