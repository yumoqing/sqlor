# -*- coding:utf8 -*-
import aiosqlite
from appPublic.argsConvert import ArgsConvert, ConditionConvert
from .sor import SQLor
from .ddl_template_sqlite import sqlite_ddl_tmpl


class SQLiteor(SQLor):
    ddl_template = sqlite_ddl_tmpl

    db2modelTypeMapping = {
        'integer': 'long',
        'int': 'long',
        'tinyint': 'short',
        'smallint': 'short',
        'mediumint': 'long',
        'bigint': 'long',
        'decimal': 'float',
        'numeric': 'float',
        'real': 'float',
        'double': 'float',
        'float': 'float',
        'char': 'char',
        'varchar': 'str',
        'text': 'text',
        'clob': 'text',
        'blob': 'bin',
        'date': 'date',
        'datetime': 'datetime',
        'boolean': 'short',
    }

    model2dbTypemapping = {
        'date': 'date',
        'time': 'time',
        'timestamp': 'datetime',
        'str': 'text',
        'char': 'char',
        'short': 'integer',
        'long': 'integer',
        'float': 'real',
        'text': 'text',
        'bin': 'blob',
        'file': 'blob',
    }

    @classmethod
    def isMe(cls, name):
        return name.lower() in ['sqlite', 'sqlite3']

    def grammar(self):
        return {
            'select': 'select',
        }

    def placeHolder(self, varname, pos=None):
        """SQLite 使用 ? 占位符"""
        if varname == '__mainsql__':
            return ''
        return '?'

    def dataConvert(self, dataList):
        if isinstance(dataList, dict):
            return tuple(dataList.values())
        else:
            return tuple(i['value'] for i in dataList)

    def pagingSQLmodel(self):
        """分页 SQL 模板"""
        return """select * from (%s) as A order by $[sort]$ limit $[rows]$ offset $[from_line]$"""

    def tablesSQL(self):
        """获取表名和备注"""
        sqlcmd = """
            SELECT 
                lower(name) AS name,
                '' AS title
            FROM sqlite_master
            WHERE type='table'
              AND name NOT LIKE 'sqlite_%';
        """
        return sqlcmd

    def fieldsSQL(self, tablename=None):
        """获取字段信息"""
        sqlcmd = f"""
            PRAGMA table_info({tablename});
        """
        # SQLite 无信息架构表，直接返回 PRAGMA 命令
        return sqlcmd

    def fkSQL(self, tablename=None):
        """获取外键信息"""
        if tablename:
            sqlcmd = f"PRAGMA foreign_key_list({tablename});"
        else:
            sqlcmd = "-- SQLite 需逐表获取外键信息"
        return sqlcmd

    def pkSQL(self, tablename=None):
        """获取主键信息"""
        sqlcmd = f"""
            SELECT name
            FROM pragma_table_info('{tablename}')
            WHERE pk != 0;
        """
        return sqlcmd

    def indexesSQL(self, tablename=None):
        """获取索引信息"""
        if not tablename:
            return "SELECT name as index_name, '' as is_unique, '' as column_name FROM sqlite_master WHERE type='index';"

        sqlcmd = f"""
            PRAGMA index_list('{tablename}');
        """
        return sqlcmd

    async def connect(self):
        """
        连接 SQLite 数据库
        dbdesc:
            path: 数据库文件路径（或 :memory:）
        """
        dbdesc = self.dbdesc
        self.dbpath = dbdesc.get('path', ':memory:')
        self.conn = await aiosqlite.connect(self.dbpath)
        self.conn.row_factory = aiosqlite.Row  # 支持 dict 访问
        self.dbname = self.dbpath

    async def close(self):
        await self.conn.close()

    async def enter(self):
        """开启事务"""
        self.cur = await self.conn.cursor()

    async def exit(self):
        """释放 cursor"""
        try:
            await self.cur.close()
        except:
            pass
        self.cur = None

