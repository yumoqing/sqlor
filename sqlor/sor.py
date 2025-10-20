import asyncio
from traceback import format_exc
import os  
import decimal
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
import sys
from datetime import datetime, date
import codecs
import re
import json
import inspect
from appPublic.worker import awaitify
from appPublic.myImport import myImport
from appPublic.jsonConfig import getConfig
from appPublic.dictObject import DictObject
from appPublic.unicoding import uDict
from appPublic.myTE import MyTemplateEngine
from appPublic.objectAction import ObjectAction
from appPublic.argsConvert import ArgsConvert,ConditionConvert
from appPublic.registerfunction import RegisterFunction
from appPublic.log import info, exception, debug
from appPublic.aes import aes_decode_b64
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
		self.dbdesc = dbdesc.copy()
		self.unpassword()
		debug(f'--------{self.dbdesc=}----')
		self.dbname = None
		self.writer = None
		self.convfuncs = {}
		self.cc = ConditionConvert()
		self.dataChanged = False
		self.metadatas={}

	def unpassword(self):
		if self.dbdesc.password:
			key=getConfig().password_key
			self.dbdesc.password = aes_decode_b64(key, self.dbdesc.password)

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
		ret = """select count(*) rcnt from (%s) rowcount_table""" % sql
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
		
	async def cur_executemany(self, cur, sql, ns):
		if inspect.iscoroutinefunction(cur.executemany):
			return await cur.executemany(sql, ns)
		f = awaitify(cur.executemany)
		return await f(sql, ns)

	async def cur_execute(self, cur, sql, ns):
		if inspect.iscoroutinefunction(cur.execute):
			ret = await cur.execute(sql, ns)
			# debug(f'-------coroutine--{ret}-{cur}----')
			return ret
		f = awaitify(cur.execute)
		ret = await f(sql, ns)
		# debug(f'------function--{ret}------')
		return ret

	async def runVarSQL(self, cursor, sql, NS):
		"""
		using a opened cursor to run a SQL statment with variable, the variable is setup in NS namespace
		return a cursor with data
		"""					
		markedSQL, datas = self.maskingSQL(sql,NS)
		datas = self.dataConvert(datas)
		try:
			return await self.cur_execute(cursor, 
									markedSQL, 
									datas)

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
		
	async def fetchone(self, cur):
		if inspect.iscoroutinefunction(cur.fetchone):
			ret = await cur.fetchone()
			# debug(f'coro:sor.fetchone()={ret}, {type(ret)}')
			return ret
		ret = await cur.fetchone()
		# debug(f'func:sor.fetchone()={ret}, {type(ret)}')
		if isinstance(ret, asyncio.Future):
			ret = ret.result()
		return ret

	async def execute(self, sql, value):
		sqltype = self.getSqlType(sql)
		cur = self.cursor()
		ret = await self.runVarSQL(cur, sql, value)
		if sqltype == 'dml':
			self.dataChanged = True
		return ret

	async def _get_data(self, sql, ns):
		cur = self.cursor()
		sqltype = self.getSqlType(sql)
		if sqltype != 'qry':
			raise Exception('not select sql')
		ret = await self.execute(sql, ns)
		fields = [i[0].lower() for i in cur.description]
		while True:
			rec = await self.fetchone(cur)
			if rec is None:
				break
			if rec is None:
				break
			if isinstance(rec, dict):
				rec = rec.values()
			dic = {}
			for i in range(len(fields)):
				v = db_type_2_py_type(rec[i])
				dic.update({fields[i]: v})
			dic = DictObject(**dic)
			yield dic

	async def executemany(self,sql,values):
		sqltype = self.getSqlType(sql)
		if sqltype != 'dml':
			raise Exception('no dml sql')
		cur = self.cursor()
		markedSQL, _ = self.maskingSQL(sql,{})
		datas = [ self.dataConvert(d) for d in values ]
		await self.cur_exectutemany(cur, markedSQL, datas)
	
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
		ret = []
		return await self.execute(sql,{})

	def isSelectSql(self,sql):
		return self.getSqlType(sql) == 'qry'

	async def record_count(self, sql, NS):
		sql = self.recordCnt(sql)
		async for r in self._get_data(sql, NS):
			t = r.rcnt
			return t
		return None

	async def pagingdata(self, sql, NS):
		paging = {
			"rowsname":"rows",
			"pagename":"page",
			"sortname":"sort"
		}
		if not NS.get('sort'):
			NS['sort'] = "id"

		sql = self.pagingSQL(sql, paging, NS)
		recs = []
		async for r in self._get_data(sql, NS):
			recs.append(r)
		return recs

	async def resultFields(self,desc,NS):
		NS.update(rows=1,page=1)
		r = await self.pagingdata(desc,NS)
		ret = [ {'name':i[0],'type':i[1]} for i in self.cur.description ]
		return ret
		
	async def sqlExe(self, sql, ns):
		sqltype = self.getSqlType(sql)
		if sqltype != 'qry':
			r = await self.execute(sql, ns)
			return r
		if 'page' in ns.keys():
			cnt = await self.record_count(sql, ns)
			rows = await self.pagingdata(sql, ns)
			return {
				'total': cnt,
				'rows': rows
			}
		ret = []
		async for r in self._get_data(sql, ns):
			ret.append(r)
		return ret

	async def sqlPaging(self,sql,ns):
		page = ns.get('page')
		if not page:
			ns['page'] = 1

		total = await self.record_count(sql,ns)
		rows = await self.pagingdata(sql,ns)
		return {
			'total':total,
			'rows':rows
		}

	async def tables(self):
		sqlstring = self.tablesSQL()
		ret = []
		async for r in self._get_data(sqlstring,{}):
			r.name = r.name.lower()
			ret.append(r)
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
		async for r in self._get_data(sqlstring, {}):
			recs.append(r)
		return recs
		
	async def fields(self,tablename=None):
		sql = self.fieldsSQL(tablename)
		recs = []
		async for r in self._get_data(sql, {}):
			recs.append(r)

		ret = []
		for r in recs:
			r.update({'type':self.db2modelTypeMapping.get(r['type'].lower(),'unknown')})
			r.update({'name':r['name'].lower()})
			ret.append(r)
		return ret
	
	async def primary(self,tablename):
		sql = self.pkSQL(tablename)
		recs = []
		async for r in self._get_data(sql, {}):
			recs.append(r)
		# debug(f'primary("{tablename}")={recs}, {sql}')
		return recs
		
	async def fkeys(self,tablename):
		sqlstring = self.fkSQL(tablename)
		recs = []
		async for r in self._get_data(sqlstring, {}):
			recs.append(r)
		
		return recs
	
	async def createTable(self,tabledesc):
		te = MyTemplateEngine([],'utf8','utf8')
		sql = te.renders(self.ddl_template,tabledesc)
		return await self.execute(sql, {})
		
	async def getTableDesc(self,tablename):
		tablename = tablename.lower()
		desc = self.getMeta(tablename)
		if desc:
			return desc
		desc = {}
		tables = await self.tables()
		summary = [i for i in tables if tablename == i.name]
		if not summary:
			e = Exception(f'table({tablename}) not exist')
			exception(f'{e}{format_exc()}')
			raise e
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
		if inspect.iscoroutinefunction(self.conn.rollback):
			await self.conn.rollback()
		else:
			self.conn.rollback()
		self.dataChanged = False

	async def commit(self):
		if inspect.iscoroutinefunction(self.conn.commit):
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
		r = await self.execute(sql,ns.copy())
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
			total = await self.record_count(sql, ns)
			rows = await self.pagingdata(sql,ns)
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
		r = await self.execute(sql, ns.copy())
		await rf.exe(f'{self.dbname}:{tablename}:u:after',ns)
		return r

	async def D(self,tablename, ns):
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
		r = await self.execute(sql, ns)
		ns = await rf.exe(f'{self.dbname}:{tablename}:d:after', ns)
		return r

	async def connect(self):
		raise Exception('Not Implemented')

	async def close(self):
		raise Exception('Not Implemented')
