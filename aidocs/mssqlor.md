# `MsSqlor` 技术文档

> **模块路径**: `.mssqlor.py`  
> **语言**: Python  
> **数据库支持**: Microsoft SQL Server (通过 `pymssql`)  
> **继承自**: `SQLor`

---

## 概述

`MsSqlor` 是一个专为 **Microsoft SQL Server** 数据库设计的 ORM（对象关系映射）辅助类，继承自通用数据库操作基类 `SQLor`。它封装了针对 SQL Server 的 DDL 模板、数据类型映射、分页查询、元数据提取等核心功能，适用于自动化建模、数据库逆向工程和动态 SQL 构建场景。

该类主要用于：
- 数据库连接识别
- 类型系统映射（数据库 ↔ 应用模型）
- 自动生成标准 SQL 语句（如查询、分页、表/字段/主键/外键/索引信息获取）

---

## 依赖说明

```python
from .sor import SQLor
from .ddl_template_sqlserver import sqlserver_ddl_tmpl
```

- `SQLor`: 所有数据库适配器的抽象基类。
- `sqlserver_ddl_tmpl`: 预定义的 SQL Server DDL 模板，用于生成建表语句。

---

## 类定义

```python
class MsSqlor(SQLor):
    ...
```

---

## 属性与常量

### 1. `ddl_template`

```python
ddl_template = sqlserver_ddl_tmpl
```

- **用途**: 定义创建表时使用的 DDL 模板。
- **值来源**: 引用外部模块 `ddl_template_sqlserver.sqlserver_ddl_tmpl`
- **典型内容示例**:
  ```sql
  CREATE TABLE [table_name] (
      [col_name] [data_type](...) NULL,
      ...
  )
  ```

---

### 2. `db2modelTypeMapping`

将 SQL Server 数据库类型映射到应用层模型字段类型。

| 数据库类型 | 模型类型 |
|-----------|----------|
| `bit`, `tinyint`, `smallint` | `'short'` |
| `bigint`, `int` | `'long'` |
| `decimal`, `numeric`, `money`, `smallmoney`, `real`, `float` | `'float'` |
| `date`, `datetime` | `'date'` |
| `timestamp`, `uniqueidentifier` | `'timestamp'` |
| `char` | `'char'` |
| `varchar`, `nvarchar`, `nchar`, `binary`, `varbinary` | `'str'` |
| `text`, `ntext` | `'text'` |
| `image` | `'file'` |

> ⚠️ 注意：`uniqueidentifier` 映射为 `'timestamp'` 可能存在语义偏差，建议根据业务需求调整。

---

### 3. `model2dbTypemapping`

将应用模型字段类型反向映射回 SQL Server 数据类型。

| 模型类型 | 数据库类型 |
|---------|------------|
| `'date'` | `datetime` |
| `'time'` | `date` *(注意：可能应为 `time` 或 `datetime`)* |
| `'timestamp'` | `timestamp` |
| `'str'` | `nvarchar` |
| `'char'` | `char` |
| `'short'` | `int` |
| `'long'` | `numeric` |
| `'float'` | `numeric` |
| `'text'` | `ntext` |
| `'file'` | `image` |

> 🔍 提示：`'long'` 和 `'float'` 均映射为 `numeric`，未指定精度，实际使用中需结合上下文补充。

---

## 类方法

### `isMe(cls, name) -> bool`

判断当前驱动是否匹配。

#### 参数
- `name` (`str`): 数据库驱动名称，例如 `'pymssql'`

#### 返回值
- `True` 当且仅当 `name == 'pymssql'`
- 否则返回 `False`

#### 示例
```python
if MsSqlor.isMe(driver_name):
    db_adapter = MsSqlor()
```

> ✅ 支持多数据库环境下自动识别 SQL Server 连接。

---

## 实例方法

### `grammar(self) -> dict`

返回支持的 SQL 语法结构。目前仅注册 `select` 语句模板。

#### 返回值
```python
{
    'select': select_stmt  # 假设全局变量或导入的 SELECT 解析器
}
```

> ⚠️ 警告：`select_stmt` 未在代码中定义，可能是遗漏或外部引用，请确保其存在。

---

### `placeHolder(self, varname, pos=None) -> str`

生成参数占位符，用于预编译 SQL。

#### 参数
- `varname` (`str`): 参数名
- `pos` (`int`, optional): 位置索引（本实现未使用）

#### 行为逻辑
- 若参数名为 `__mainsql__`，返回空字符串（通常用于嵌入原始 SQL）
- 其他情况统一返回 `%s` —— 符合 `pymssql` 参数化语法

#### 示例
```python
cursor.execute(sql % (), data)
```

> ✅ 安全地防止 SQL 注入。

---

### `dataConvert(self, dataList) -> tuple`

将输入数据标准化为可执行的元组格式。

#### 输入类型处理
- **字典类型**：取 `.values()` 并转为列表 → 元组
- **列表 of 字典**：提取每个元素的 `'value'` 字段 → 元组

#### 示例
```python
# 输入1: {'a': 1, 'b': 2} → (1, 2)
# 输入2: [{'value': 1}, {'value': 2}] → (1, 2)
```

#### 返回值
- 标准化的 `tuple`，可用于 `cursor.execute(..., params)`

---

### `pagingSQLmodel(self) -> str`

返回适用于 SQL Server 的分页查询模板（基于 `ROW_NUMBER()` 窗口函数）。

#### 模板结构
```sql
SELECT *
FROM (
    SELECT ROW_NUMBER() OVER(ORDER BY $[sort]$) AS _row_id, page_s.*
    FROM (%s) page_s
) A
WHERE _row_id >= $[from_line]$ AND _row_id < $[end_line]$
```

#### 占位符说明
| 占位符         | 含义                   |
|----------------|------------------------|
| `%s`           | 子查询（原 SQL）       |
| `$[sort]$`     | 排序字段               |
| `$[from_line]$`| 起始行号（含）         |
| `$[end_line]$` | 结束行号（不含）       |

#### 使用方式
此模板需配合字符串替换工具填充实际值，常用于构建分页接口。

> ✅ 兼容 SQL Server 2005+ 版本。

---

### `tablesSQL(self) -> str`

获取当前数据库所有用户表及其标题（描述）。

#### 查询语句
```sql
SELECT 
    LOWER(d.name) AS name,
    LOWER(CAST(ISNULL(f.VALUE, d.name) AS NVARCHAR)) AS title
FROM sysobjects d
LEFT JOIN sys.extended_properties f ON d.id = f.major_id AND f.minor_id = 0
WHERE d.xtype = 'U'
```

#### 字段说明
- `name`: 表名（小写）
- `title`: 表备注 / 描述（若无则用表名代替）

> 📌 `xtype = 'U'` 表示用户表。

---

### `fieldsSQL(self, tablename=None) -> str`

获取指定表或全部表的字段元数据。

#### 查询语句摘要
```sql
SELECT 
    name = LOWER(a.name),
    type = b.name,
    length = COLUMNPROPERTY(a.id, a.name, 'PRECISION'),
    dec = ISNULL(COLUMNPROPERTY(a.id, a.name, 'Scale'), NULL),
    nullable = CASE WHEN a.isnullable = 1 THEN 'yes' ELSE 'no' END,
    title = LOWER(CAST(ISNULL(g.[value], a.name) AS NVARCHAR)),
    table_name = LOWER(d.name)
FROM syscolumns a
...
WHERE schema_name(schema_id) = 'dbo'
```

#### 条件控制
- 若传入 `tablename`，则添加 `WHERE LOWER(d.name) = 'xxx'`
- 最终按 `a.id, a.colorder` 排序（保证列顺序）

#### 返回字段
| 字段        | 说明                     |
|-------------|--------------------------|
| `name`      | 列名（小写）             |
| `type`      | 数据类型                 |
| `length`    | 精度（字符长度或数字总位数） |
| `dec`       | 小数位数                 |
| `nullable`  | 是否可为空（yes/no）     |
| `title`     | 列说明（扩展属性）       |
| `table_name`| 所属表名（小写）         |

> ✅ 支持从 `sys.extended_properties` 获取注释。

---

### `fkSQL(self, tablename=None) -> str`

获取外键关系（引用其他表的主键作为本表外键）。

#### 查询逻辑
查找以某表为主表（被引用）的所有外键关联。

```sql
SELECT
    MainCol.name AS field,          -- 主表列名（被引用）
    oSub.name AS fk_table,          -- 子表名称（引用方）
    SubCol.name AS fk_field         -- 子表列名（外键列）
FROM sys.foreign_keys fk
JOIN ... -- 多表连接定位主子表及列
```

#### 条件
- 可选过滤：`lower(oMain.name) = 'xxx'`，即只查某主表被哪些表引用

#### 示例输出
| field | fk_table | fk_field |
|-------|----------|----------|
| id    | orders   | user_id  |

> 💡 适用于构建实体关系图（ERD）。

---

### `pkSQL(self, tablename=None) -> str`

获取主键字段信息。

#### 查询语句
```sql
SELECT 
    LOWER(a.table_name) AS table_name,
    LOWER(b.column_name) AS field_name
FROM information_schema.table_constraints a
INNER JOIN information_schema.constraint_column_usage b
    ON a.constraint_name = b.constraint_name
WHERE a.constraint_type = 'PRIMARY KEY'
```

#### 可选过滤
- 若提供 `tablename`，追加条件：`AND LOWER(a.table_name) = 'xxx'`

#### 输出
每条记录表示一个主键列。

---

### `indexesSQL(self, tablename=None) -> str`

获取索引信息（包括唯一性与包含的列）。

#### 查询语句
```sql
SELECT
    index_name = LOWER(IDX.Name),
    index_type = IDX.is_unique,
    column_name = LOWER(C.Name)
FROM sys.indexes IDX
INNER JOIN sys.index_columns IDXC ...
INNER JOIN sys.columns C ...
WHERE O.type = 'U' AND O.is_ms_shipped = 0
```

#### 条件
- 排除系统对象（`is_ms_shipped=0`）
- 可选按表名过滤：`LOWER(O.name) = 'xxx'`

#### 输出字段
| 字段         | 说明                         |
|--------------|------------------------------|
| `index_name` | 索引名（小写）               |
| `index_type` | 是否唯一（1=唯一；0=非唯一） |
| `column_name`| 索引包含的列名               |

> ✅ 支持复合索引拆解为多行展示。

---

## 使用示例

### 获取所有表
```python
db = MsSqlor(connection)
sql = db.tablesSQL()
cursor.execute(sql)
tables = cursor.fetchall()
for t in tables:
    print(t['name'], t['title'])
```

### 获取某表字段
```python
sql = db.fieldsSQL('users')
cursor.execute(sql)
fields = cursor.fetchall()
for f in fields:
    print(f['name'], f['type'], f['nullable'])
```

### 分页查询构造
```python
base_sql = "SELECT id, name FROM users"
pager = db.pagingSQLmodel() % base_sql
pager = pager.replace('$[sort]$', 'id') \
             .replace('$[from_line]$', '1') \
             .replace('$[end_line]$', '11')
cursor.execute(pager)
```

---

## 已知限制与建议

| 问题 | 建议 |
|------|------|
| `select_stmt` 未定义 | 确保已在作用域内定义或导入 |
| `time` 模型映射为 `date` | 应改为 `time` 或 `datetime` |
| `uniqueidentifier` 映射为 `timestamp` | 语义错误，建议新增 `'guid'` 类型 |
| 不支持模式（schema）切换 | 当前固定 `schema_name='dbo'`，如需扩展建议增加参数 |
| 缺少 `create`, `insert` 等语法支持 | 可在 `grammar()` 中逐步扩展 |

---

## 总结

`MsSqlor` 是一个功能完整的 SQL Server 数据库适配器，具备以下优势：

✅ 自动识别驱动  
✅ 完善的类型双向映射  
✅ 强大的元数据查询能力（表、列、主键、外键、索引）  
✅ 标准化的分页模板  
✅ 支持参数化查询与安全执行

适合集成于 ORM 框架、数据库管理工具或低代码平台中，实现对 SQL Server 的自动化操作。

--- 

> 📎 文档版本：v1.0  
> 📅 更新日期：2025年4月5日