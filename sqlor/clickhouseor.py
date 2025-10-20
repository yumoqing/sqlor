# -*- coding:utf8 -*-
# pip install clickhouse-driver
from clickhouse_driver import Client
from appPublic.argsConvert import ArgsConvert, ConditionConvert
from .sor import SQLor
from .ddl_template_clickhouse import clickhouse_ddl_tmpl
from .const import ROWS

class ClickHouseor(SQLor):
    ddl_template = clickhouse_ddl_tmpl

    db2modelTypeMapping = {
        'int8': 'short',
        'int16': 'short',
        'int32': 'long',
        'int64': 'long',
        'float32': 'float',
        'float64': 'float',
        'decimal': 'float',
        'string': 'str',
        'date': 'date',
        'datetime': 'datetime',
        'uuid': 'str',
        'bool': 'short',
    }

    model2dbTypemapping = {
        'short': 'Int32',
        'long': 'Int64',
        'float': 'Float64',
        'str': 'String',
        'char': 'String',
        'date': 'Date',
        'datetime': 'DateTime',
        'timestamp': 'DateTime',
        'text': 'String',
        'bin': 'String',
        'file': 'String',
    }

    @classmethod
    def isMe(self, name):
        return name.lower() in ('clickhouse', 'clickhouse_driver')

    def placeHolder(self, varname, pos=None):
        if varname == '__mainsql__':
            return ''
        return '%s'

    def dataConvert(self, dataList):
        if isinstance(dataList, dict):
            d = [i for i in dataList.values()]
        else:
            d = [i['value'] for i in dataList]
        return tuple(d)

    def pagingSQLmodel(self):
        # ClickHouse 支持 LIMIT offset, size
        return """SELECT * FROM (%s) AS A LIMIT $[from_line]$, $[rows]$"""

    def tablesSQL(self):
        return """SELECT name, comment AS title
FROM system.tables
WHERE database = '%s'""" % self.dbdesc.get('dbname', 'default')

    def fieldsSQL(self, tablename=None):
        sql = """SELECT 
    name AS name,
    type AS type,
    NULL AS length,
    NULL AS dec,
    'yes' AS nullable,
    comment AS title,
    '%s' AS table_name
FROM system.columns
WHERE database = '%s'
""" % (tablename or '', self.dbdesc.get('dbname', 'default'))
        if tablename:
            sql += " AND table = '%s';" % tablename
        return sql

    def pkSQL(self, tablename=None):
        # ClickHouse 没有 system.keys 表，用 order_by_keys 替代
        sql = """SELECT name FROM system.columns
WHERE database = '%s' AND table = '%s' AND is_in_primary_key = 1;
""" % (self.dbdesc.get('dbname', 'default'), tablename.lower())
        return sql

    def indexesSQL(self, tablename=None):
        # ClickHouse 没有传统索引
        return """SELECT name, 'order_by' AS index_name, 'primary' AS is_unique, name AS column_name
FROM system.columns
WHERE database = '%s' AND table = '%s' AND is_in_primary_key = 1;
""" % (self.dbdesc.get('dbname', 'default'), tablename.lower())

    def fkSQL(self, tablename=None):
        # ClickHouse 不支持外键
        return "SELECT 'ClickHouse does not support foreign keys' AS msg;"

	async def connect(self):
		self.conn = Client(**self.dbdesc)
		self.cur = self.conn
	
	async def close(self):
		self.conn.close()
	
