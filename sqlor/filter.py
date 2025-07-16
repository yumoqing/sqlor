# -*- coding:utf8 -*- 
"""
过滤器解释器
解释json格式的SQL查询过滤
过滤支持：
双目关系运算符：
	AND
	OR
单目关系运算符：
	NOT
表达式：
	表达关系有：=,<>,>,<,>=,<=,in,not in
	a=b格式的
{
	"and":[
		{
			"field":'field1'
			"op":"=",
			"const":1
			"var":"var1"
		},
		{
			"or":[...]
		}
		{
			"not":{...}
		}
	]
}
"""
try:
	import ujson as json
except:
	import json

class DBFilter(object):
	operators = [
		"=",
		"!=",
		"IN",
		"NOT IN",
		">",
		">=",
		"<",
		"<=",
		"IS NULL",
		"IS NOT NULL",
		"LIKE",
		"NOT LIKE"
	]
	logic = [
		"AND",
		"OR",
		"NOT"
	]
	def __init__(self,filterjson):
		if isinstance(filterjson, str):
			filterjson = json.loads(filterjson)
		self.filterjson = filterjson
		
	def gen(self, ns={}):
		self.consts = {}
		ret = self._genFilterSQL(self.filterjson, ns)
		ns.update(self.consts)
		return ret
	
	def get_variables(self):
		return self.get_filters_variables(self.filterjson)

	def get_filters_variables(self, filters):
		vs = {}
		keys = [ k for k in filters.keys() ]
		if len(keys) == 1:
			fs = filters[keys[0]]
			if isinstance(fs, list):
				for f in fs:
					v = self.get_filters_variables(f)
					if v:
						vs.update(v)
				return vs
			else:
				v = self.get_filters_variables(fs)
				if v:
					vs.update(v)
				return vs
		if 'var' in keys:
			return {
				filters['var']:filters['field']
			}
		return None

	def _genFilterSQL(self,fj, ns):
		keys = [ i for i in fj.keys()]
		if len(keys) == 1:
			key = keys[0]
			if key.lower() in ['and','or']:
				if type(fj[key]) != type([]) or len(fj[key])<2:
					raise Exception(key + ':' + json.dumps(fj[key]) + ':is not a array, or array length < 2')
				subsqls = [self._genFilterSQL(f, ns) for f in fj[key]]
				subsqls = [ s for s in subsqls if s is not None]
				if len(subsqls) < 1:
					return None
				if len(subsqls) < 2:
					return subsqls[0]

				a  = ' %s ' % key
				sql =  a.join(subsqls)
				if key == 'or':
					return ' (%s) ' % sql
				return sql
			if key.lower() == 'not':
				if type(fj[key]) != type({}):
					raise Exception(key + ':' + json.dumps(fj[key]) + ':is not a dict')
				a  = ' %s ' % key
				sql = self._genFilterSQL(fj[key], ns)
				if not sql:
					return None
				return ' not (%s) ' % sql
		return self._genFilterItems(fj, ns)
			
	def _genFilterItems(self,fj, ns):
		keys = fj.keys()
		assert 'field' in keys
		assert 'op' in keys
		assert 'const' in keys or 'var' in keys  or fj['op'].upper() in ['IS NULL', 'IS NOT NULL']
		op = fj.get('op').upper()
		assert op in self.operators

		if op in  ['IS NULL', 'IS NOT NULL']:
			return f"{fj['field']} {op}"

		var = fj.get('var')
		if var and not var in ns.keys():
			return None
			
		if 'const' in keys:
			cnt = len(self.consts.keys())
			name = f'filter_const_{cnt}'
			self.consts.update({name:fj.get('const')})
			sql = '%s %s ${%s}$' % (fj.get('field'), fj.get('op'), name)
			return sql
		
		sql = '%s %s ${%s}$' % (fj.get('field'), fj.get('op'), fj.get('var'))
		return sql

def default_filterjson(fields: list, ns: dict):
	fj = {}
	c = [ {
		'field':k,
		'op':'=',
		'var':k
		} for k in ns.keys() if k in fields ]
	print(f'default_filterjson():{c=}')
	if len(c) < 1:
		return None
	if len(c) < 2:
		return c[0]
	return {
		'AND':c
	}
	
if __name__ == '__main__':
	fj = {
		"AND":[
		{
			"field":'field1',
			"op":"=",
			"var":"name"
		},
		{
			"field":'del_flg',
			"op":"=",
			"const":"0"
		}]
	}
	
	dbf = DBFilter(fj)
	print(fj)
	print(dbf.gen({'name':'joe'}))
	print(f'{dbf.consts=}')
