# -*- coding:utf8 -*-
from .dbpools import DBPools
from .filter import DBFilter
from appPublic.objectAction import ObjectAction
from appPublic.dictObject import DictObject
from appPublic.timeUtils import  date2str,time2str,str2Date
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

class CRUD(object):
	def __init__(self,dbname,tablename,rows=10):
		self.pool = DBPools()
		self.dbname = dbname
		self.tablename = tablename
		self.rows = rows
		self.oa = ObjectAction()
		
	async def primaryKey(self):
		data = await self.pool.getTablePrimaryKey(self.dbname,self.tablename)
		return data
	
	async def forignKeys(self):
		data = self.pool.getTableForignKeys(self.dbname,self.tablename)
		return data
		
	async def I(self):
		"""
		fields information
		"""
		fields = await self.primaryKey()
		pks = [ i.field_name for i in fields ]
		data = await self.pool.getTableFields(self.dbname,self.tablename)
		[ d.update({'primarykey':True}) for d in data if d.name in pks ]
		data = self.oa.execute(self.dbname+'_'+self.tablename,'tableInfo',data)
		return data
	
	async def fromStr(self,data):
		fields = await getTableFields(self.dbname,self.tablename)
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
		fields = await getTableFields(self.dbname,self.tablename)
		ret = {}
		for k in data:
			for f in fields:
				if k == f.name:
					ret[k] = data[k]
					f = toStringFuncs.get(f.type,None)
					if f is not None and data[k] is not None:
						ret[k] = f(data[k])
		return ret
		
	async def datagrid(self,request,target):
		fields = await self.I()
		fs = [ self.defaultIOField(f) for f in fields ]
		id = self.dbname+':'+ self.tablename
		pk = await self.primaryKey()
		idField = pk[0].field
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

	async def C(self,rec):
		"""
		create new data
		"""
		fields = await self.pool.getTableFields(self.dbname,self.tablename)
		flist = [ f['name'] for f in fields ]
		fns = ','.join(flist)
		vfs = ','.join([ '${' + f + '}$' for f in flist ])
		data = {}
		[ data.update({k.lower():v}) for k,v in rec.items() ]
		@self.pool.runSQL
		def addSQL(dbname,data):
			sqldesc={
				"sql_string" : """
				insert into %s (%s) values (%s)
				""" % (self.tablename,fns,vfs),
			}
			return sqldesc
			
		data = self.oa.execute(self.dbname+'_'+self.tablename,'beforeAdd',data)
		await addSQL(self.dbname,data)
		data = self.oa.execute(self.dbname+'_'+self.tablename,'afterAdd',data)
		return data
	
	async def defaultFilter(self,NS):
		fields = await self.pool.getTableFields(self.dbname,self.tablename)
		d = [ '%s = ${%s}$' % (f.name,f.name) for f in fields if f.name in NS.keys() ]
		if len(d) == 0:
			return ''
		ret = ' and ' + ' and '.join(d)
		return ret

	def R(self,filters=None,NS={}):
		"""
		retrieve data
		"""
		@self.pool.runSQLIterator
		def retrieve(dbname,data):
			fstr = ''
			if filters is not None:
				fstr = ' and '
				dbf = DBFilter(filters)
				fstr = fstr + dbf.genFilterString()
			else:
				fstr = self.defaultFilter(NS)
			sqldesc = {
				"sql_string":"""select * from %s where 1=1 %s""" % (self.tablename,fstr),
			}
			return sqldesc
			
		
		data = self.oa.execute(self.dbname+'_'+self.tablename,'beforeRetieve',NS)
		data = await retrieve(self.dbname,data,fstr)
		data = self.oa.execute(self.dbname+'_'+self.tablename,'afterRetieve',data)
		return data
		
	async def RP(self,filters=None,NS={}):
		@self.pool.runPaging
		def pagingdata(dbname,data,filterString):
			fstr = ""
			if filters is not None:
				fstr = ' and '
				dbf = DBFilter(filters)
				fstr = fstr + dbf.genFilterString()
			else:
				fstr = self.defaultFilter(NS)
				
			sqldesc = {
				"sql_string":"""select * from %s where 1=1 %s""" % (self.tablename,filterString),
				"default":{'rows':self.rows}
			}
			return sqldesc
			
		if not NS.get('sort',False):
			fields = await self.pool.getTableFields(self.dbname,self.tablename)
			NS['sort'] = fields[0]['name']
		d = await pagingdata(self.dbname,NS)
		return d

	async def U(self,data):
		"""
		update  data
		"""
		@self.pool.runSQL
		def update(dbname,NS,condi,newData):
			c = [ '%s = ${%s}$' % (i,i) for i in condi ]
			u = [ '%s = ${%s}$' % (i,i) for i in newData ]
			cs = ' and '.join(c)
			us = ','.join(u)
			sqldesc = {
				"sql_string":"""update %s set %s where %s""" % (self.tablename,us,cs)
			}
			return sqldesc
		
		pk = await self.primaryKey()
		pkfields = [k.field_name for k in pk ]
		newData = [ k for k in data if k not in pkfields ]
		data = self.oa.execute(self.dbname+'_'+self.tablename,'beforeUpdate',data)
		await update(self.dbname,data,pkfields,newData)
		data = self.oa.execute(self.dbname+'_'+self.tablename,'afterUpdate',data)
		return data
	
	async def D(self,data):
		"""
		delete data
		"""
		@self.pool.runSQL
		def delete(dbname,data,fields):
			c = [ '%s = ${%s}$' % (i,i) for i in fields ]
			cs = ' and '.join(c)
			sqldesc = {
				"sql_string":"delete from %s where %s" % (self.tablename,cs)
			}
			return sqldesc

		pk = await self.primaryKey()
		pkfields = [k.field_name for k in pk ]
		data = self.oa.execute(self.dbname+'_'+self.tablename,'beforeDelete',data)
		await delete(self.dbname,data,pkfields)
		data = self.oa.execute(self.dbname+'_'+self.tablename,'afterDelete',data)
		return data

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
		
