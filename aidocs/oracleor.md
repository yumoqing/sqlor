# `Oracleor` 类技术文档

```markdown
# Oracle 数据库操作类：`Oracleor`

`Oracleor` 是一个用于操作 Oracle 数据库的 Python 类，继承自 `SQLor` 基类。该类封装了与 Oracle 数据库交互所需的核心功能，包括数据类型映射、分页查询、元数据获取（表、字段、主键、外键、索引等）、占位符处理以及 SQL 模板管理。

---

## 继承关系

- **父类**: `SQLor`
- **所在模块**: `.sor.SQLor`
- **用途**: 提供通用 SQL 操作接口

## 依赖项

```python
from .sor import SQLor
from .ddl_template_oracle import oracle_ddl_tmpl
```

- `oracle_ddl_tmpl`: Oracle 特定的 DDL（数据定义语言）模板，用于生成建表语句等。

---

## 类定义

```python
class Oracleor(SQLor):
    ddl_template = oracle_ddl_tmpl
    ...
```

---

## 核心属性

### 1. `ddl_template`

```python
ddl_template = oracle_ddl_tmpl
```

- **说明**: 使用 Oracle 专用的 DDL 模板来生成建表语句。
- **用途**: 支持通过模型自动生成数据库表结构。

---

### 2. `db2modelTypeMapping`

将 Oracle 数据库字段类型映射为应用层模型类型（如 Python 或 ORM 模型类型）。

| Oracle 类型 | 映射模型类型 |
|------------|-------------|
| `char`                     | `char`       |
| `nchar`                    | `str`        |
| `varchar`, `varchar2`, `nvarchar2` | `str`        |
| `number`, `integer`        | `long`       |
| `binary_float`, `binary_double`, `float` | `float`     |
| `timestamp`, `timestamp with time zone`, `timestamp with local time zone`, `interval day to second` | `timestamp` |
| `interval year to moth`    | `date`       |
| `clob`, `nclob`            | `text`       |
| `blob`, `bfile`            | `file`       |
| `date`                     | `date`       |

> ⚠️ 注意：`interval year to moth` 存在拼写错误，应为 `interval year to month`。

---

### 3. `model2dbTypemapping`

将模型类型反向映射为 Oracle 数据库字段类型。

| 模型类型 | 映射 Oracle 类型 |
|---------|------------------|
| `date`, `time`, `timestamp` | `date`         |
| `str`                      | `varchar2`     |
| `char`                     | `char`         |
| `short`, `long`, `float`   | `number`       |
| `text`                     | `nclob`        |
| `file`                     | `blob`         |

---

## 类方法

### `@classmethod isMe(cls, name)`

判断当前驱动是否为 Oracle 驱动。

#### 参数：
- `name` (str): 数据库连接使用的驱动名称。

#### 返回值：
- `True` 如果 `name == 'cx_Oracle'`，否则 `False`。

#### 示例：
```python
if Oracleor.isMe('cx_Oracle'):
    print("This is Oracle database.")
```

---

## 实例方法

### `grammar(self)`

返回当前数据库支持的 SQL 语法结构（目前仅支持 `select`）。

#### 返回值：
```python
{
    'select': select_stmt
}
```

> ⚠️ 注意：`select_stmt` 未在代码中定义，可能是外部导入或遗漏。

---

### `placeHolder(self, varname, pos=None)`

生成适用于 Oracle 的参数占位符（使用命名绑定变量）。

#### 参数：
- `varname` (str): 变量名。
- `pos` (int, optional): 位置参数（未使用）。

#### 规则：
- 若 `varname == '__mainsql__'`，返回空字符串。
- 否则返回 `:%s` 格式，例如 `:username`。

#### 示例：
```python
self.placeHolder('username')  # 输出: ':username'
```

---

### `dataConvert(self, dataList)`

将数据库原始结果列表转换为字典格式。

#### 参数：
- `dataList`: 可以是字典或包含字段信息的列表（如 `[{'name': 'id', 'value': 1}, ...]`）。

#### 返回值：
- 转换后的字典，键为字段名，值为对应值。

#### 示例：
```python
input_data = [{'name': 'id', 'value': 1}, {'name': 'name', 'value': 'Alice'}]
result = obj.dataConvert(input_data)
# result => {'id': 1, 'name': 'Alice'}
```

---

### `pagingSQLmodel(self)`

返回 Oracle 分页查询的 SQL 模板（基于 `ROWNUM` 实现）。

#### 返回模板：
```sql
select * 
from (
    select page_s.*, rownum row_id 
    from (%s) page_s 
    order by $[sort]$
) 
where row_id >= $[from_line]$ and row_id < $[end_line]$
```

#### 占位符说明：
- `%s`: 子查询（原始 SQL）
- `$[sort]$`: 排序字段
- `$[from_line]$`: 起始行号（从 1 开始）
- `$[end_line]$`: 结束行号（不包含）

> ✅ 适用于 Oracle 9i 及以上版本。

---

### `tablesSQL(self)`

生成查询当前用户所有表及其注释的 SQL。

#### 返回 SQL：
```sql
select 
lower(table_name) as name,
lower(decode(comments,null,table_name,comments)) as title
from USER_TAB_COMMENTS where table_type = 'TABLE'
```

#### 字段说明：
- `name`: 表名（小写）
- `title`: 表标题/注释，若无注释则用表名代替

> 查询来源：`USER_TAB_COMMENTS`

---

### `fieldsSQL(self, tablename=None)`

查询指定表的所有字段信息。

#### 参数：
- `tablename` (str, optional): 表名（不区分大小写）

#### 返回 SQL：
```sql
select lower(utc.COLUMN_NAME) name
,utc.DATA_TYPE type
,utc.DATA_LENGTH length
,utc.data_scale dec
,case when utc.nullable = 'Y' then 'yes' else 'no' end nullable
,lower(nvl(ucc.comments,utc.COLUMN_NAME)) title
,lower(utc.table_name) as table_name
from  user_tab_cols utc left join USER_COL_COMMENTS ucc 
on utc.table_name = ucc.table_name and utc.COLUMN_NAME = ucc.COLUMN_NAME
```

如果提供了 `tablename`，会添加过滤条件：
```sql
where lower(utc.table_name) = 'xxx'
```

#### 字段说明：
| 字段 | 含义 |
|------|------|
| `name`      | 字段名（小写） |
| `type`      | 数据类型 |
| `length`    | 字段长度 |
| `dec`       | 小数位数（scale） |
| `nullable`  | 是否可为空（'yes'/'no'） |
| `title`     | 字段注释，无则用字段名替代 |
| `table_name`| 所属表名（小写） |

> 数据源：`user_tab_cols`, `USER_COL_COMMENTS`

---

### `fkSQL(self, tablename=None)`

查询外键约束信息。

#### 参数：
- `tablename` (str, optional): 限定查询某一张表的外键

#### 返回 SQL：
```sql
select
 distinct(ucc.column_name) as field,
 rela.table_name as fk_table,
 rela.column_name as fk_field
from
 user_constraints uc,
 user_cons_columns ucc,
 (
   select t2.table_name, t2.column_name, t1.r_constraint_name 
   from user_constraints t1, user_cons_columns t2 
   where t1.r_constraint_name = t2.constraint_name 
 ) rela
where
 uc.constraint_name = ucc.constraint_name
 and uc.r_constraint_name = rela.r_constraint_name
```

若有表名，则追加：
```sql
and lower(uc.table_name) = 'xxx'
```

#### 返回字段：
- `field`: 当前表的外键字段
- `fk_table`: 引用的目标表
- `fk_field`: 目标表中的被引用字段

> 用于构建表间关联关系。

---

### `pkSQL(self, tablename=None)`

查询主键字段信息。

#### 参数：
- `tablename` (str, optional): 限定查询某张表的主键

#### 返回 SQL：
```sql
select
 lower(col.table_name) table_name,
 lower(col.column_name) as field_name
from
 user_constraints con,
 user_cons_columns col
where
 con.constraint_name = col.constraint_name 
 and con.constraint_type = 'P'
```

若有表名，则追加：
```sql
and lower(col.table_name) = 'xxx'
```

#### 返回字段：
- `table_name`: 表名（小写）
- `field_name`: 主键字段名（小写）

> `constraint_type='P'` 表示主键约束。

---

### `indexesSQL(self, tablename=None)`

查询索引信息。

#### 参数：
- `tablename` (str, optional): 限定查询某张表的索引

#### 返回 SQL：
```sql
select 
  lower(a.index_name) index_name,
  lower(a.UNIQUENESS) is_unique,
  lower(b.column_name) column_name 
from user_indexes a, user_ind_columns b
where a.index_name = b.index_name
```

若有表名，则追加：
```sql
and lower(a.table_name) = lower('xxx')
```

#### 返回字段：
- `index_name`: 索引名称（小写）
- `is_unique`: 是否唯一（`unique` / `non-unique`）
- `column_name`: 索引包含的字段名（小写）

> 支持多字段索引（每行一个字段）。

---

## 总结

`Oracleor` 类为 Oracle 数据库提供了完整的元数据读取和 SQL 构造能力，适用于：

- 自动生成模型（ORM 映射）
- 可视化数据库结构工具
- 动态 SQL 执行与分页
- 数据迁移与同步系统

其设计遵循抽象与具体分离原则，便于扩展至其他数据库。

---
> 📌 **备注**：部分变量（如 `select_stmt`）可能依赖外部定义，请确保上下文完整。
```