以下是针对你提供的 Python 代码编写的 **Markdown 格式技术文档**，适用于项目内部文档或开发者参考手册。

---

# `MySqlor` 类技术文档

## 概述

`MySqlor` 是一个用于操作 MySQL 数据库的 SQL 构建与元数据提取类，继承自 `SQLor` 基类。它封装了与 MySQL 相关的 DDL 模板、类型映射、分页查询、表结构查询等功能，支持从数据库中提取表、字段、主键、外键、索引等元信息，并提供模型与数据库之间的类型转换机制。

该类主要配合 `pymysql` 驱动使用，通过动态生成 SQL 查询语句实现对数据库结构的反向工程（Reverse Engineering）和 ORM 映射支持。

---

## 继承关系

```python
class MySqlor(SQLor)
```

- 父类：`SQLor`
- 所属模块：`.sor.SQLor`

---

## 导入依赖

```python
from appPublic.argsConvert import ArgsConvert, ConditionConvert
from .sor import SQLor
from .const import ROWS
from .ddl_template_mysql import mysql_ddl_tmpl
```

> - `ArgsConvert`, `ConditionConvert`: 参数与条件转换工具。
> - `ROWS`: 分页常量定义。
> - `mysql_ddl_tmpl`: MySQL 的 DDL 模板（如建表语句模板）。

---

## 类属性

### `ddl_template`

```python
ddl_template = mysql_ddl_tmpl
```

- 类型：`dict`
- 描述：MySQL 的 DDL 建表语句模板，用于根据模型生成建表 SQL。
- 来源：导入自 `ddl_template_mysql.py` 文件中的 `mysql_ddl_tmpl` 变量。

---

### `db2modelTypeMapping`

将 MySQL 数据库字段类型映射为应用层模型类型（Model Type）。

| 数据库类型         | 模型类型     |
|--------------------|--------------|
| tinyint            | short        |
| smallint           | short        |
| mediumint          | long         |
| int                | long         |
| bigint             | long         |
| decimal            | float        |
| double             | float        |
| float              | float        |
| char               | char         |
| varchar            | str          |
| tinyblob / tinytext| text         |
| mediumblob / mediumtext | text    |
| blob / text        | text         |
| longblob           | bin          |
| longtext           | text         |
| binary / varbinary | text         |
| date               | date         |
| time               | time         |
| datetime           | datetime     |
| timestamp          | datestamp    |
| year               | short        |

> **说明**：
> - BLOB 类型统一映射为 `bin` 或 `text`，取决于大小。
> - 时间类型做了精细化区分，例如 `timestamp` 映射为 `datestamp`，便于后续处理。

---

### `model2dbTypemapping`

将模型类型映射回数据库字段类型。

| 模型类型   | 数据库类型     |
|------------|----------------|
| date       | date           |
| time       | date           |
| timestamp  | timestamp      |
| str        | varchar        |
| char       | char           |
| short      | int            |
| long       | bigint         |
| float      | double         |
| text       | longtext       |
| bin        | longblob       |
| file       | longblob       |

> **注意**：`time` 类型在数据库中仍以 `date` 存储，可能需结合上下文解析；实际应考虑使用 `time` 类型优化。

---

## 类方法

### `isMe(cls, name)`

判断当前数据库驱动是否匹配。

#### 参数
- `name` (`str`)：数据库连接驱动名称（如 `'pymysql'`）

#### 返回值
- `bool`：若为 `'pymysql'` 则返回 `True`，否则 `False`

#### 示例
```python
if MySqlor.isMe('pymysql'):
    print("This is MySQL")
```

---

## 实例方法

### `grammar(self)`

> ⚠️ **当前存在语法错误！**

此方法尝试返回一个包含 `select` 语句模板的字典，但引用了未定义的变量 `select_stmt`。

#### 代码问题
```python
def grammar(self):
    return {
        'select': select_stmt,  # ❌ NameError: name 'select_stmt' is not defined
    }
```

#### 建议修复
应确保 `select_stmt` 已在作用域内定义，或改为字符串形式：

```python
def grammar(self):
    return {
        'select': 'SELECT $[fields]$ FROM $[table]$ WHERE $[where]$',
    }
```

---

### `placeHolder(self, varname, pos=None)`

返回参数占位符格式（用于预编译 SQL）。

#### 参数
- `varname` (`str`)：变量名
- `pos` (`int`, optional)：位置参数索引（未使用）

#### 返回值
- 若 `varname == '__mainsql__'`，返回空字符串 `''`
- 其他情况返回标准占位符 `'%s'`（适配 `pymysql`）

#### 用途
用于构建参数化查询，防止 SQL 注入。

#### 示例
```python
cursor.execute(sql, values)  # values 将以 %s 替代
```

---

### `dataConvert(self, dataList)`

将输入数据标准化为元组格式，供 SQL 执行使用。

#### 参数
- `dataList` (`dict` 或 `list[dict]`)：待转换的数据

#### 返回值
- `tuple`：提取出的值组成的元组

#### 行为逻辑
- 如果是字典 → 提取 `.values()`
- 如果是字典列表 → 提取每个元素的 `'value'` 字段
- 最终转为 `tuple`

#### 示例
```python
data1 = {'a': 1, 'b': 2}
# 结果: (1, 2)

data2 = [{'value': 1}, {'value': 2}]
# 结果: (1, 2)
```

---

### `pagingSQLmodel(self)`

返回 MySQL 分页查询模板（基于 `LIMIT offset, count`）。

#### 当前代码问题
```python
def pagingSQLmodel(self):
    return """..."""
    return """..."""  # ❌ 第二个 return 永远不会执行
```

#### 正确版本（推荐）
```python
def pagingSQLmodel(self):
    return """
        SELECT * FROM (%s) A 
        ORDER BY $[sort]$
        LIMIT $[from_line]$, $[rows]$
    """
```

#### 占位符说明
| 占位符         | 含义                   |
|----------------|------------------------|
| `%s`           | 子查询原始 SQL         |
| `$[sort]$`     | 排序字段（如 `id ASC`）|
| `$[from_line]$`| 起始行偏移量（offset） |
| `$[rows]$`     | 每页记录数（limit）    |

#### 示例输出
```sql
SELECT * FROM (
    SELECT id, name FROM users
) A
ORDER BY id DESC
LIMIT 10, 20
```

---

### `tablesSQL(self)`

生成查询当前数据库所有表名及其注释的 SQL。

#### 返回值
- `str`：查询 `INFORMATION_SCHEMA.TABLES` 的 SQL 语句

#### SQL 内容
```sql
SELECT 
    lower(TABLE_NAME) AS name, 
    lower(TABLE_COMMENT) AS title 
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'your_db_name'
```

#### 动态填充
- 使用 `self.dbdesc.get('dbname', 'unknown')` 获取数据库名

#### 输出字段
| 字段  | 说明           |
|-------|----------------|
| name  | 小写表名       |
| title | 表注释（小写） |

---

### `fieldsSQL(self, tablename=None)`

查询指定表的所有字段元信息。

#### 参数
- `tablename` (`str`, optional)：表名，若不传则查询所有表

#### 返回值
- `str`：查询 `information_schema.columns` 的 SQL

#### SQL 片段
```sql
SELECT
    lower(column_name) AS name,
    data_type AS type,
    CASE 
        WHEN character_maximum_length IS NULL THEN NUMERIC_PRECISION
        ELSE character_maximum_length 
    END AS length,
    NUMERIC_SCALE AS dec,
    lower(is_nullable) AS nullable,
    column_comment AS title,
    lower(table_name) AS table_name
FROM information_schema.columns
WHERE lower(TABLE_SCHEMA) = 'your_dbname'
  AND lower(table_name) = 'your_table'  -- 可选
```

#### 输出字段说明
| 字段         | 说明                             |
|--------------|----------------------------------|
| name         | 字段名（小写）                   |
| type         | 数据类型                         |
| length       | 长度或精度                       |
| dec          | 小数位数                         |
| nullable     | 是否可为空（yes/no）             |
| title        | 字段注释                         |
| table_name   | 所属表名（小写）                 |

---

### `fkSQL(self, tablename=None)`

查询外键约束信息。

#### 参数
- `tablename` (`str`, optional)：仅查询与该表相关的外键

#### 返回值
- `str`：查询外键关系的 SQL

#### SQL 逻辑
联查三张系统表：
- `KEY_COLUMN_USAGE`
- `TABLES`
- `REFERENTIAL_CONSTRAINTS`

#### 查询内容
| 字段                 | 说明                     |
|----------------------|--------------------------|
| 拥有者               | Schema 名称              |
| 父表名称             | 被引用表                 |
| 父表字段             | 被引用列                 |
| 子表名称             | 引用表                   |
| 子表字段             | 引用列                   |
| 约束名               | 外键约束名称             |
| 表注释               | 子表的注释               |
| 约束更新规则         | ON UPDATE 规则           |
| 约束删除规则         | ON DELETE 规则           |

> **过滤条件**：只返回 `REFERENCED_TABLE_NAME IS NOT NULL` 的记录（即真正的外键）

---

### `pkSQL(self, tablename=None)`

查询指定表的主键字段。

#### 参数
- `tablename` (`str`)：必须传入表名

#### 返回值
- `str`：查询主键字段的 SQL

#### SQL
```sql
SELECT DISTINCT column_name AS name 
FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
WHERE table_name='your_table' 
  AND constraint_name='PRIMARY'
```

#### 注意事项
- `table_name` 会被强制转为小写
- 使用 `DISTINCT` 避免重复

---

### `indexesSQL(self, tablename=None)`

查询索引信息（包括唯一索引）。

#### 参数
- `tablename` (`str`, optional)：指定表名

#### 返回值
- `str`：获取索引元数据的 SQL

#### SQL
```sql
SELECT DISTINCT
    lower(index_name) AS index_name,
    CASE NON_UNIQUE
        WHEN 1 THEN 'unique'
        ELSE ''
    END AS is_unique,
    lower(column_name) AS column_name
FROM information_schema.statistics
WHERE table_schema = 'your_dbname'
  AND table_name = 'your_table'  -- 可选
```

#### 输出字段
| 字段         | 说明                                   |
|--------------|----------------------------------------|
| index_name   | 索引名称（小写）                       |
| is_unique    | 是否唯一索引（值为 `'unique'` 或空）   |
| column_name  | 索引包含的字段（单个字段一行）         |

> **注意**：复合索引会拆分为多行显示。

---

## 使用示例

```python
# 初始化 MySqlor 实例
dbdesc = {'dbname': 'testdb'}
mysqlor = MySqlor(dbdesc=dbdesc)

# 获取所有表
sql_tables = mysqlor.tablesSQL()
print(sql_tables)

# 获取某表字段
sql_fields = mysqlor.fieldsSQL('users')
print(sql_fields)

# 获取主键
sql_pk = mysqlor.pkSQL('users')

# 分页模板
paging_model = mysqlor.pagingSQLmodel()
final_sql = paging_model % "(SELECT * FROM users)" \
    .replace("$[sort]$", "id DESC") \
    .replace("$[from_line]$", "0") \
    .replace("$[rows]$", "10")
```

---

## 已知问题与改进建议

| 问题 | 描述 | 建议 |
|------|------|------|
| `grammar()` 方法报错 | 引用了未定义的 `select_stmt` | 定义常量或抛出 `NotImplementedError` |
| `pagingSQLmodel()` 多余返回 | 第二个 `return` 不可达 | 删除无效行 |
| `dataConvert()` 缺少健壮性检查 | 对非 dict/list 输入无保护 | 添加类型校验和异常处理 |
| `placeHolder` 忽略 `pos` 参数 | 参数未使用 | 可移除或用于未来扩展 |

---

## 总结

`MySqlor` 是一个功能完整的 MySQL 元数据操作类，具备以下能力：

✅ 类型双向映射  
✅ 表/字段/主键/外键/索引信息提取  
✅ 分页 SQL 模板支持  
✅ 参数占位符兼容 `pymysql`  
✅ 支持模型驱动开发（MDD / ORM）

建议结合模板引擎（如 Jinja2）进一步提升 SQL 拼接灵活性。

--- 

> 📝 文档版本：v1.0  
> 📅 更新时间：2025-04-05  
> © 项目公共组件库团队