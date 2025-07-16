# -*- coding:utf8 -*-
from .dbpools import DBPools
from .const import ROWS
from .filter import DBFilter
from appPublic.objectAction import ObjectAction
from appPublic.dictObject import DictObject
from appPublic.timeUtils import  date2str,time2str,str2Date
from appPublic.uniqueID import getID
toStringFuncs={
	'char':None,
	'str':None,
	'short':str,
	'long':str,
	'float':str,
	'date':date2str,
	'time':time2str,
}
fromStringFuncs={
	'short':int,
	'long':int,
	'float':float,
	'date':str2Date,
	'time':str2Date
}

class DatabaseNotfound(Exception):
	def __init__(self,dbname):
		Exception.__init__(self)
		self.dbname = dbname

	def __str__(self):
		return f'{self.dbname} not found'

class CRUD(object):
	def __init__(self,dbname,tablename,rows=ROWS):
		self.pool = DBPools()
		if dbname not in self.pool.databases.keys():
			raise DatabaseNotfound(dbname)
		self.dbname = dbname
		self.tablename = tablename
		self.rows = rows
		self.primary_data = None
		self.oa = ObjectAction()
		
	async def primaryKey(self,**kw):
		if self.primary_data is None:
			self.primary_data = await self.pool.getTablePrimaryKey(self.dbname,
							self.tablename,**kw)
		
		return self.primary_data
	
	async def forignKeys(self,**kw):
		data = self.pool.getTableForignKeys(self.dbname,self.tablename,**kw)
		return data
		
	async def I(self,**kw):
		"""
		fields information
		"""
		@self.pool.inSqlor
		async def main(dbname,NS,**kw):
			pkdata = await self.primaryKey(**kw)
			pks = [ i.field_name for i in pkdata ]
			data = await self.pool.getTableFields(self.dbname,self.tablename,**kw)
			for d in data:
				if d.name in pks:
					d.update({'primarykey':True})
			data = self.oa.execute(self.dbname+'_'+self.tablename,'tableInfo',data)
			return data

		return await main(self.dbname,{},**kw)
	
	async def fromStr(self,data):
		fields = await self.pool.getTableFields(self.dbname,self.tablename)
		ret = {}
		for k in data:
			v = None if data[k] == '' else data[k]
			for f in fields:
				if k == f.name:
					ret[k] = v
					f = fromStringFuncs.get(f.type,None)
					if f is not None and v is not None:
						ret[k] = f(v)
		return ret
	
	async def toStr(self,data):
		fields = await self.pool.getTableFields(self.dbname,self.tablename)
		ret = {}
		for k in data:
			for f in fields:
				if k == f.name:
					ret[k] = data[k]
					f = toStringFuncs.get(f.type,None)
					if f is not None and data[k] is not None:
						ret[k] = f(data[k])
		return ret
		
	async def datagrid(self,request,targeti,**kw):
		fields = await self.I()
		fs = [ self.defaultIOField(f) for f in fields ]
		id = self.dbname+':'+ self.tablename
		pk = await self.primaryKey(**kw)
		idField = pk[0]['field_name']
		data = {
			"tmplname":"widget_js.tmpl",
			"data":{
				"__ctmpl__":"datagrid",
				"__target__":target,
				"data":{
					"name":id,
					"icon-conv":"icon-table",
					"title":tablename,
					"url":absurl('./RP.dspy?id=%s' % id),
					"deleteUrl":absurl('./D.dspy?id=%s' % id),
					"addUrl":absurl('./C.dspy?id=%s' % id),
					"updateUrl":absurl('./U.dspy?id=%s' % id),
					"idField":idField,
					"dnd":True,
					"view":"scrollview",
					"fields":fs,
					"toolbar":{
						"tools":[
							{
								"name":"add",
								"icon":"icon-add",
								"label":"add ball"
							},
							{
								"name":"delete",
								"icon":"icon-delete",
								"label":"delete ball"
							},
							{
								"name":"moveup",
								"icon":"icon-up",
								"label":"moveup ball"
							},
							{
								"name":"movedown",
								"icon":"icon-down",
								"label":"movedown ball"
							}
						]
					},
					"options":{
						"pageSize":50,
						"pagination":False
					}
				}
			}
		}
		data = self.oa.execute(id,'datagrid',data)
		return data
		
	def defaultIOField(self,f):
		if f.type in ['str']:
			return {
				"primarykey":f.get('primarykey',False),
				"name":f.name,
				"hidden":False,
				"sortable":True,
				"label":f.title,
				"align":"center",
				"iotype":"text"	
			}
		if f.type in ['float','short','long']:
			return {
				"primarykey":f.get('primarykey',False),
				"sortable":True,
				"name":f.name,
				"hidden":False,
				"label":f.title,
				"align":"right",
				"iotype":"text"	
			}
		return {
			"primarykey":f.get('primarykey',False),
			"name":f.name,
			"sortable":True,
			"hidden":False,
			"label":f.title,
			"align":"center",
			"iotype":"text"	
		}

	async def C(self,rec,**kw):
		"""
		create new data
		"""
		@self.pool.runSQL
		async def addSQL(dbname,data,**kw):
			fns = kw['fns']
			vfs = kw['vfs']
			sqldesc={
				"sql_string" : """
				insert into %s (%s) values (%s)
				""" % (self.tablename,fns,vfs),
			}
			return sqldesc
			
		@self.pool.inSqlor
		async def main(dbname,NS,**kw):
			fields = await self.pool.getTableFields(self.dbname,self.tablename,**kw)
			flist = [ f['name'] for f in fields ]
			fns = ','.join(flist)
			vfs = ','.join([ '${' + f + '}$' for f in flist ])
			data = {}
			[ data.update({k.lower():v}) for k,v in NS.items() ]
			pk = await self.primaryKey(**kw)
			k = pk[0]['field_name']
			if not data.get(k):
				v = getID()
				data[k] = v
			data = self.oa.execute(self.dbname+'_'+self.tablename,'beforeAdd',data)
			kwargs = kw.copy()
			kwargs['fns'] = fns
			kwargs['vfs'] = vfs
			await addSQL(self.dbname,data,**kwargs)
			data = self.oa.execute(self.dbname+'_'+self.tablename,'afterAdd',data)
			return {k:data[k]}

		return await main(self.dbname,rec,**kw)
	
	async def defaultFilter(self,NS,**kw):
		fields = await self.pool.getTableFields(self.dbname,self.tablename,**kw)
		d = [ '%s = ${%s}$' % (f['name'],f['name']) for f in fields if f['name'] in NS.keys() ]
		if len(d) == 0:
			return ''
		ret = ' and ' + ' and '.join(d)
		return ret

	async def R(self,filters=None,NS={},**kw):
		"""
		retrieve data
		"""
		@self.pool.runSQL
		async def retrieve(dbname,data,**kw):
			fstr = ''
			if filters is not None:
				fstr = ' and '
				dbf = DBFilter(filters)
				fstr = fstr + dbf.genFilterString()
			else:
				fstr = await self.defaultFilter(NS,**kw)
			sqldesc = {
				"sql_string":"""select * from %s where 1=1 %s""" % (self.tablename,fstr),
			}
			return sqldesc
			
		@self.pool.runSQLPaging
		async def pagingdata(dbname,data,filters=None,**kw):
			fstr = ""
			if filters is not None:
				fstr = ' and '
				dbf = DBFilter(filters)
				fstr = fstr + dbf.genFilterString()
			else:
				fstr = await self.defaultFilter(NS,**kw)
				
			sqldesc = {
				"sql_string":"""select * from %s where 1=1 %s""" % (self.tablename,fstr),
				"default":{'rows':self.rows}
			}
			return sqldesc
		
		@self.pool.inSqlor
		async def main(dbname,NS,**kw):
			p = await self.primaryKey(**kw)
			if NS.get('__id') is not None:
				NS[p[0]['field_name']] = NS['__id']
				del NS['__id']
				if NS.get('page'):
					del NS['page']

			if NS.get('page'):
				if NS.get('sort',None) is None:
					NS['sort'] = p[0]['field_name']

			data = self.oa.execute(self.dbname+'_'+self.tablename,'beforeRetrieve',NS)
			if NS.get('page'):
				data = await pagingdata(self.dbname,data,**kw)
			else:
				data = await retrieve(self.dbname,data,**kw)
			data = self.oa.execute(self.dbname+'_'+self.tablename,'afterRetrieve',data)
			return data

		return await main(self.dbname,NS,**kw)
		
	async def U(self,data, **kw):
		"""
		update  data
		"""
		@self.pool.runSQL
		async def update(dbname,NS,**kw):
			condi = [ i['field_name'] for i in self.primary_data ]
			newData = [ i for i in NS.keys() if i not in condi ]
			c = [ '%s = ${%s}$' % (i,i) for i in condi ]
			u = [ '%s = ${%s}$' % (i,i) for i in newData ]
			cs = ' and '.join(c)
			us = ','.join(u)
			sqldesc = {
				"sql_string":"""update %s set %s where %s""" % (self.tablename,us,cs)
			}
			return sqldesc
		
		@self.pool.inSqlor
		async def main(dbname,NS,**kw):
			pk = await self.primaryKey(**kw)
			pkfields = [k.field_name for k in pk ]
			newData = [ k for k in data if k not in pkfields ]
			data = self.oa.execute(self.dbname+'_'+self.tablename,'beforeUpdate',data)
			await update(self.dbname,data,**kw)
			data = self.oa.execute(self.dbname+'_'+self.tablename,'afterUpdate',data)
			return data
		return await main(self.dbname,data,**kw)
	
	async def D(self,data,**kw):
		"""
		delete data
		"""
		@self.pool.runSQL
		def delete(dbname,data,**kw):
			pnames = [ i['field_name'] for i in self.primary_data ]
			c = [ '%s = ${%s}$' % (i,i) for i in pnames ]
			cs = ' and '.join(c)
			sqldesc = {
				"sql_string":"delete from %s where %s" % (self.tablename,cs)
			}
			return sqldesc

		@self.pool.inSqlor
		async def main(dbname,NS,**kw):
			data = self.oa.execute(self.dbname+'_'+self.tablename,'beforeDelete',data)
			await delete(self.dbname,data,pkfields,**kw)
			data = self.oa.execute(self.dbname+'_'+self.tablename,'afterDelete',data)
			return data
		return await main(self.dbname,data,**kw)

if __name__ == '__main__':
	DBPools({
		"ambi":{
			"driver":"pymssql",
			"coding":"utf-8",
			"dbname":"ambi",
			"kwargs":{
				"user":"ymq",
				"database":"ambi",
				"password":"ymq123",
				"host":"localhost"
			}
		},
		"metadb":{
			"driver":"pymssql",
			"coding":"utf-8",
			"dbname":"metadb",
			"kwargs":{
				"user":"ymq",
				"database":"metadb",
				"password":"ymq123",
				"host":"localhost"
			}
		}
	})
	crud = CRUD('ambi')
	#fields = crud.I('cashflow')
	#for i in fields:
	#	print(i)
	
	data = crud.RP('cashflow')
	print(data.total)
	for i in data.rows:
		print(i.balance,i.asid)
		
