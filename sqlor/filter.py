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
			"lv":{
				"tblabbr":"a1",
				"field":"field1"
			}
			"op":"=",
			"rv":{
				"type":"field",
				"tblabbr":"table1"
				"field":"field2"
			}
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
import ujson as json

class DBFilter(object):
	def __init__(self,filterjson):
		self.filterjson = filterjson
		
	def save(self,fpath):
		pass
		
	def getArguments(self):
		pass
	
	def genFilterString(self):
		ret = self._genFilterSQL(self.filterjson)
		return ret
	
	def _genFilterSQL(self,fj):
		keys =  fj.keys()
		if len(keys) == 1:
			key = keys[0]
			if key.lower() in ['and','or']:
				if type(fj[key]) != type([]) or len(fj[key])<2:
					raise Exception(key + ':' + json.dumps(fj[key]) + ':is not a array, or array length < 2')
				a  = ' %s ' % key
				return a.join([self._genFilterSQL(f) for f in fj[key] ])
			if key.lower() == 'not':
				if type(fj[key]) != type({}):
					raise Exception(key + ':' + json.dumps(fj[key]) + ':is not a dict')
				a  = ' %s ' % key
				return a + self._genFilterSQL(fj[key])
		return self._genFilterItems(fj)
			
	def _genFilterItems(self,fj):
		keys = fj.keys()
		assert 'lv' in keys
		assert 'op' in keys
		assert 'rv' in keys
		op = fj['op'].lower()
		assert op in ['=','<>','>','<','>=','<=','in','not in']
		return self._genFilterFieldValue(fj['lv']) + ' ' + fj['op'] + ' ' + self._genFilterRightValue(fj['rv'])

	def _genFilterFieldValue(self,fj):
		keys = fj.keys()
		assert 'field' in keys
		ret = fj['field']
		if 'tblabbr' in keys:
			ret = fj['tblabbr'] + '.' + ret
		return  ret

	def _genFilterRightValue(self,fj):
		keys = fj.keys()
		assert 'type' in keys
		if fj['type'] == 'field':
			return self._genFilterFieldValue(fj)
		if fj['type'] == 'const':
			return self._genFilterConstValue(fj)
		if fj['type'] == 'parameter':
			return self._getFilterParameterValue(fj)
	
	def _genFilterConstValue(self,fj):
		keys = fj.keys()
		assert 'value' in keys
		return fj['value']
	
	def _getFilterParameterValue(self,fj):
		keys = fj.keys()
		assert 'parameter' in keys
		return fj['parameter']


