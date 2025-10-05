以下是为提供的 Python 代码编写的 **Markdown 格式技术文档**，适用于项目文档或 API 参考手册。

---

# `PostgreSQLor` 类技术文档

## 概述

`PostgreSQLor` 是一个用于操作 PostgreSQL 数据库的数据库抽象类（继承自 `SQLor`），提供了对 PostgreSQL 特有语法、数据类型映射、元数据查询等功能的支持。该类主要用于生成 SQL 语句、处理占位符、转换数据结构以及获取表结构信息等任务。

该类主要配合 ORM 或代码生成器使用，支持从数据库中提取表、字段、主键、外键、索引等元数据，并将其映射为模型层可用的数据格式。

---

## 继承关系

```python
class PostgreSQLor(SQLor)
```

- 父类：`SQLor`
- 当前类：`PostgreSQLor`

---

## 导入依赖

```python
from .sor import SQLor
from .ddl_template_postgresql import postgresql_ddl_tmpl
```

- `SQLor`: 基础数据库操作抽象类。
- `postgresql_ddl_tmpl`: PostgreSQL 的 DDL 模板，用于建表语句生成。

---

## 类属性

### `ddl_template`

```python
ddl_template = postgresql_ddl_tmpl
```

- 说明：指定 PostgreSQL 的 DDL 建表语句模板。
- 用途：在生成 `CREATE TABLE` 等语句时引用此模板。

---

### `db2modelTypeMapping`

将 PostgreSQL 数据库中的字段类型映射到应用模型中的类型。

```python
db2modelTypeMapping = {
    'smallint': 'short',
    'integer': 'long',
    'bigint': 'llong',
    'decimal': 'float',
    'numeric': 'float',
    'real': 'float',
    'double': 'float',
    'serial': 'long',
    'bigserial': 'llong',
    'char': 'char',
    'character': 'char',
    'varchar': 'str',
    'character varying': 'str',
    'text': 'text',
    'timestamp': 'timestamp',
    'date': 'date',
    'time': 'time',
    'boolean': 'char',
    'bytea': 'file'
}
```

| 数据库类型 | 模型类型 |
|-----------|----------|
| smallint  | short    |
| integer   | long     |
| bigint    | llong    |
| numeric/decimal/real/double | float |
| serial/bigserial | long/llong |
| char/character | char |
| varchar/character varying | str |
| text      | text     |
| timestamp | timestamp|
| date      | date     |
| time      | time     |
| boolean   | char (0/1) |
| bytea     | file (二进制存储) |

> ⚠️ 注意：`boolean` 被映射为 `char`，通常以 `'0'/'1'` 表示布尔值。

---

### `model2dbTypemapping`

将模型中的字段类型反向映射回 PostgreSQL 的数据库类型。

```python
model2dbTypemapping = {
    'date': 'date',
    'time': 'date',  # 注意：time 映射为 date 类型？可能需确认是否合理
    'timestamp': 'timestamp',
    'str': 'varchar',
    'char': 'char',
    'short': 'smallint',
    'long': 'integer',
    'float': 'numeric',
    'text': 'text',
    'file': 'bytea',
}
```

> ❗ 注意：`model2dbTypemapping['time'] = 'date'` 存在逻辑问题，应为 `'time'`，可能是 bug。

---

## 类方法

### `isMe(cls, name)`

判断当前数据库驱动是否匹配 PostgreSQL。

#### 参数：
- `name` (`str`)：数据库连接使用的驱动名称。

#### 返回值：
- `bool`：若 `name` 为 `'psycopg2'` 或 `'pyguass'`，返回 `True`；否则返回 `False`。

#### 示例：
```python
PostgreSQLor.isMe('psycopg2')  # True
PostgreSQLor.isMe('sqlite3')   # False
```

> 支持标准 PostgreSQL 驱动 `psycopg2` 和国产化替代品 `pyguass`（如达梦、高斯等兼容版）。

---

## 实例方法

### `grammar(self)`

返回当前数据库支持的 SQL 语法结构定义（目前仅包含 `select`）。

#### 返回值：
```python
{
    'select': select_stmt
}
```

> ⚠️ 注意：`select_stmt` 未在代码中定义，可能是外部导入变量，需确保上下文存在。

---

### `placeHolder(self, varname, i)`

生成参数化查询中的占位符。

#### 参数：
- `varname` (`str`)：参数名。
- `i` (`int`)：参数索引（当前未使用）。

#### 返回值：
- 若 `varname == '__mainsql__'`，返回空字符串。
- 否则返回 `%({varname})s` 格式的命名占位符（符合 `psycopg2` 参数风格）。

#### 示例：
```python
obj.placeHolder('username', 0)  # '%(username)s'
obj.placeHolder('__mainsql__', 0)  # ''
```

---

### `dataConvert(self, dataList)`

将输入数据统一转换为字典格式。

#### 参数：
- `dataList`：可以是字典或对象列表（每个元素含 `name` 和 `value` 字段）。

#### 返回值：
- `dict`：键为字段名，值为对应值。

#### 示例：
```python
data = [{'name': 'id', 'value': 1}, {'name': 'name', 'value': 'Alice'}]
converted = obj.dataConvert(data)
# 结果: {'id': 1, 'name': 'Alice'}

obj.dataConvert({'x': 1})  # 直接返回原字典
```

---

### `pagingSQLmodel(self)`

返回分页 SQL 模板（⚠️ **此处有严重问题**）。

#### 返回值：
```sql
select * 
from (
    select page_s.*, rownum row_id 
    from (%s) page_s 
    order by $[sort]$
) 
where row_id >= $[from_line]$ and row_id < $[end_line]$
```

> ❌ 错误分析：
> - `rownum` 是 Oracle 的伪列，**PostgreSQL 不支持**。
> - 正确的 PostgreSQL 分页应使用 `LIMIT` 和 `OFFSET`。
>
> ✅ 正确写法建议：
```sql
SELECT * FROM (%s) AS page_s 
ORDER BY $[sort]$ 
LIMIT $[page_size]$ OFFSET $[offset]$
```

> 📝 提示：此方法需要重构以适配 PostgreSQL 分页机制。

---

### `tablesSQL(self)`

生成查询当前数据库所有表及其描述的 SQL。

#### 返回值（SQL）：
```sql
select x.name, y.description as title
from 
  (select a.name, c.oid
   from (select lower(tablename) as name from pg_tables where schemaname='public') a,
        pg_class c
   where a.name = c.relname) x
left join pg_description y
  on x.oid = y.objoid and y.objsubid = '0'
```

#### 功能说明：
- 查询 `public` 模式下的所有表名（小写）。
- 左连接 `pg_description` 获取表注释（`title`）。
- `objsubid = '0'` 表示表级注释（非字段注释）。

#### 返回字段：
- `name`: 表名（小写）
- `title`: 表注释（可为空）

---

### `fieldsSQL(self, tablename=None)`

生成查询指定表所有字段信息的 SQL。

#### 参数：
- `tablename` (`str`)：表名（不区分大小写）

#### 返回值（SQL）：
```sql
SELECT 
    a.attname AS name, 
    t.typname AS type, 
    case t.typname
        when 'varchar' then a.atttypmod - 4
        when 'numeric' then (a.atttypmod - 4) / 65536
        else null
    end as length,
    case t.typname
        when 'numeric' then (a.atttypmod - 4) % 65536
        else null
    end as dec,
    case a.attnotnull
        when 't' then 'no'
        when 'f' then 'yes'
    end as nullable,
    b.description AS title
FROM pg_class c, pg_attribute a
    LEFT JOIN pg_description b ON a.attrelid = b.objoid AND a.attnum = b.objsubid,
    pg_type t
WHERE lower(c.relname) = '%s'
    AND a.attnum > 0
    AND a.attrelid = c.oid
    AND a.atttypid = t.oid
ORDER BY a.attnum;
```

#### 字段说明：

| 字段 | 含义 |
|------|------|
| `name` | 字段名 |
| `type` | 数据类型（如 varchar, int4） |
| `length` | 字段长度（varchar 最大长度，numeric 总位数） |
| `dec` | 小数位数（numeric 类型） |
| `nullable` | 是否可为空（`yes` 表示可空） |
| `title` | 字段注释 |

> 🔍 技术细节：
> - `atttypmod - 4` 是 PostgreSQL 中提取 `varchar(n)` 和 `numeric(p,s)` 定义长度的方式。
> - `attnum > 0` 排除系统列（如 OID）。

---

### `fkSQL(self, tablename=None)`

⚠️ **注意：当前实现错误！**

#### 当前 SQL 使用了 `user_constraints`、`user_cons_columns` —— 这些是 **Oracle** 的系统视图！

PostgreSQL 中并不存在这些视图。

#### 正确实现应类似如下：

```sql
SELECT
    tc.column_name AS field,
    ccu.table_name AS fk_table,
    ccu.column_name AS fk_field
FROM
    information_schema.table_constraints AS tc
    JOIN information_schema.foreign_key_columns AS fkc
        ON tc.constraint_name = fkc.constraint_name
    JOIN information_schema.constraint_column_usage AS ccu
        ON fkc.unique_constraint_name = ccu.constraint_name
WHERE
    tc.constraint_type = 'FOREIGN KEY'
    AND tc.table_schema = 'public'
    AND tc.table_name = %s
```

> ❌ 当前方法无法在 PostgreSQL 上运行，请尽快修复。

---

### `pkSQL(self, tablename=None)`

生成查询指定表主键字段的 SQL。

#### 参数：
- `tablename` (`str`)：表名（不区分大小写）

#### 返回值（SQL）：
```sql
select 
    pg_attribute.attname as field_name,
    lower(pg_class.relname) as table_name
from pg_constraint 
    inner join pg_class on pg_constraint.conrelid = pg_class.oid 
    inner join pg_attribute on pg_attribute.attrelid = pg_class.oid 
        and pg_attribute.attnum = pg_constraint.conkey[1]
    inner join pg_type on pg_type.oid = pg_attribute.atttypid
where lower(pg_class.relname) = '%s' 
  and pg_constraint.contype = 'p'
```

> ⚠️ 限制：只取第一个主键字段（`conkey[1]`），不支持复合主键完整提取。

#### 建议改进：
```sql
-- 使用 unnest(conkey) 提取所有主键字段
SELECT a.attname AS field_name
FROM pg_index ix
JOIN pg_attribute a ON a.attrelid = ix.indrelid AND a.attnum = ANY(ix.indkey)
WHERE ix.indrelid = '"%s"'::regclass AND ix.indisprimary
ORDER BY a.attnum;
```

---

### `indexesSQL(self, tablename=None)`

生成查询指定表所有索引信息的 SQL。

#### 参数：
- `tablename` (`str`)：表名（小写）

#### 返回值（SQL）：
```sql
select
    i.relname as index_name,
    case ix.INDISUNIQUE
        when 't' then 'unique'
        else ''
    end as is_unique,
    a.attname as column_name
from
    pg_class t,
    pg_class i,
    pg_index ix,
    pg_attribute a
where
    t.oid = ix.indrelid
    and i.oid = ix.indexrelid
    and a.attrelid = t.oid
    and a.attnum = ANY(ix.indkey)
    and t.relkind = 'r'
    and lower(t.relname) = '%s'
order by
    t.relname,
    i.relname
```

#### 返回字段：
- `index_name`: 索引名称
- `is_unique`: 是否唯一索引（'unique' 或空）
- `column_name`: 索引对应的字段名

> ✅ 支持多字段索引拆解显示。

---

## 总结与改进建议

| 项目 | 状态 | 建议 |
|------|------|------|
| 类型映射 | ✅ 完整 | 可增加数组、JSON 类型支持 |
| `placeHolder` | ✅ 兼容 psycopg2 | 良好 |
| `dataConvert` | ✅ 实用 | 建议添加类型校验 |
| `pagingSQLmodel` | ❌ 错误 | 替换为 `LIMIT/OFFSET` |
| `fkSQL` | ❌ 使用 Oracle 语法 | 重写为 `information_schema` 版本 |
| `pkSQL` | ⚠️ 仅支持单主键 | 扩展为支持复合主键 |
| `tablesSQL`, `fieldsSQL`, `indexesSQL` | ✅ 正确可用 | 可优化性能 |

---

## 许可与维护

- 维护者：未知（请补充）
- 适用版本：PostgreSQL 9.6+
- 依赖驱动：`psycopg2`, `pyguass`（兼容版）
- 所属模块：`.sor.postgresql`

--- 

✅ **建议后续升级方向**：
- 使用 `information_schema` 替代部分 `pg_*` 系统表以提高可移植性。
- 添加单元测试覆盖各 SQL 查询。
- 引入日志记录和异常处理机制。

--- 

*文档版本：1.0*  
*最后更新：2025-04-05*