以下是为提供的 `SQLite3or` 类编写的 **Markdown 格式技术文档**，适用于项目开发文档、API 说明或团队协作参考。

---

# `SQLite3or` 技术文档

## 概述

`SQLite3or` 是一个基于 `SQLor` 基类的数据库适配器类，专用于 **SQLite3** 数据库的操作。它实现了对 SQLite 数据库的元数据查询（如表、字段、主键、索引等）以及 SQL 片段生成的支持，主要用于 ORM 或通用数据库访问层中识别和操作 SQLite 数据库结构。

该类通过映射数据库类型与模型类型，提供统一的数据抽象接口。

继承自：`.sor.SQLor`

---

## 类定义

```python
class SQLite3or(SQLor):
```

---

## 类属性

### `db2modelTypeMapping`

将 SQLite 数据库字段类型映射为应用层模型字段类型。

| 数据库类型 | 模型类型 |
|-----------|----------|
| `text`    | `str`    |
| `blob`    | `file`   |
| `int`     | `long`   |
| `integer` | `long`   |
| `real`    | `float`  |

> 示例：`'TEXT' -> 'str'`, `'BLOB' -> 'file'`

### `model2dbTypemapping`

将模型字段类型映射回 SQLite 的数据库类型。

| 模型类型   | 数据库类型 |
|-----------|------------|
| `date`     | `text`     |
| `time`     | `text`     |
| `timestamp`| `text`     |
| `str`      | `text`     |
| `char`     | `text`     |
| `short`    | `int`      |
| `long`     | `int`      |
| `float`    | `real`     |
| `text`     | `text`     |
| `file`     | `blob`     |

> 用于建表或类型转换时使用。

---

## 类方法

### `isMe(name)`

判断当前适配器是否匹配指定数据库类型名称。

#### 参数：
- `name` (`str`)：数据库标识名。

#### 返回值：
- `bool`：当 `name == 'sqlite3'` 时返回 `True`，否则 `False`。

#### 示例：
```python
SQLite3or.isMe('sqlite3')  # True
SQLite3or.isMe('mysql')    # False
```

---

## 实例方法

### `placeHolder(varname, pos=None)`

返回 SQL 占位符格式，SQLite 使用 `?` 作为参数占位符。

#### 参数：
- `varname` (`str`)：变量名。
- `pos` (`int`, 可选)：位置索引（未使用）。

#### 返回值：
- `str`：若 `varname` 为 `'__mainsql__'` 返回空字符串；其余情况返回 `'?'`。

#### 示例：
```python
db.placeHolder('username')        # '?'
db.placeHolder('__mainsql__')     # ''
```

---

### `dataConvert(dataList)`

将输入数据标准化为元组形式，供执行 SQL 时传参。

#### 参数：
- `dataList` (`dict` 或 `list[dict]`)：待处理的数据。

#### 行为：
- 若是字典：取其所有 `.values()`。
- 若是字典列表：提取每个元素的 `'value'` 字段。

#### 返回值：
- `tuple`：标准化后的值元组。

#### 示例：
```python
db.dataConvert({'a': 1, 'b': 2}) 
# -> (1, 2)

db.dataConvert([{'value': 1}, {'value': 2}])
# -> (1, 2)
```

---

### `pagingSQLmodel()`

获取分页 SQL 模板，支持 `$[sort]$`, `$[from_line]$`, `$[end_line]$` 动态替换。

#### 返回值：
- `str`：分页查询模板字符串。

```sql
select * from (%s) order by $[sort]$ limit $[from_line]$,$[end_line]$
```

> `%s` 将被子查询填充，`$[]$` 为后续模板引擎替换标记。

---

### `tablesSQL()`

生成查询数据库中所有表的 SQL 语句。

#### 返回值：
```sql
select name, tbl_name as title from sqlite_master where upper(type) = 'TABLE'
```

> 查询 `sqlite_master` 系统表，获取表名及其别名（title）。

---

### `fieldsSQL(tablename)`

**（注意：当前实现有缺陷）**

本应返回用于获取表结构信息的 SQL，但实际未正确构造语句。

#### 当前代码：
```python
return sqlcmd  # sqlcmd 为空字符串
```

#### 正确预期（建议修复）：
```python
return f"PRAGMA table_info('{tablename}')"
```

> 用于获取表字段详情（cid, name, type, notnull, dflt_value, pk）

---

### `fields(tablename)`

解析指定表的所有字段信息，并进行标准化处理。

#### 参数：
- `tablename` (`str`)：表名。

#### 处理流程：
1. 调用 `fieldsSQL(tablename)` 获取结构查询 SQL。
2. 执行 SQL 并收集结果。
3. 使用正则提取字段类型的长度与精度（如 `VARCHAR(50,2)`）。
4. 映射数据库类型 → 模型类型（通过 `db2modelTypeMapping`）。
5. 统一字段名为小写，添加 `title` 字段。

#### 返回值：
`list[dict]`，每个字段对象包含：

| 键       | 类型   | 描述 |
|----------|--------|------|
| `name`   | str    | 字段名（小写） |
| `type`   | str    | 模型类型（如 str/long/file） |
| `length` | int    | 类型长度（括号内第一个数字） |
| `dec`    | int    | 小数位数（第二个数字） |
| `title`  | str    | 别名，默认等于字段名 |
| `pk`     | int    | 是否为主键（来自 PRAGMA 输出） |

> ⚠️ 注意：由于 `fieldsSQL` 未正确实现，此方法在运行时会出错。

---

### `fkSQL(tablename)`

获取外键相关信息的 SQL 语句。

#### 当前实现：
```python
return ""
```

> **尚未实现**，SQLite 支持外键需开启 `PRAGMA foreign_keys=ON`，可通过 `PRAGMA foreign_key_list(table)` 查询。

---

### `fkeys(tablename)`

返回指定表的外键信息列表。

#### 返回值：
- `[]`：空列表（表示暂不支持外键提取）。

> 预期未来可返回类似：
```python
[
    {
        "field": "user_id",
        "ref_table": "users",
        "ref_field": "id"
    }
]
```

---

### `pkSQL(tablename)`

获取主键信息的 SQL 语句。

#### 当前实现：
```python
return ""
```

> **未实现**，推荐使用 `PRAGMA table_info(tablename)` 中的 `pk` 字段判断。

---

### `primary(tablename)`

从字段信息中提取主键字段。

#### 参数：
- `tablename` (`str`)

#### 实现逻辑：
1. 调用 `self.fields(tablename)` 获取所有字段。
2. 过滤出 `pk == 1` 的字段。
3. 返回字段名列表。

#### 返回值：
`list[dict]`，例如：
```python
[{'field': 'id'}, {'field': 'code'}]
```

> ⚠️ 依赖 `fields()` 正常工作。

---

### `indexesSQL(tablename=None)`

生成查询索引信息的 SQL。

#### 参数：
- `tablename` (`str`, 可选)：若指定，则只查该表的索引。

#### 返回值：
```sql
select * from sqlite_master 
where lower(type) = 'index'
```

如果传入 `tablename`，附加条件：
```sql
and lower(tbl_name)='xxx'
```

> 查询系统表 `sqlite_master` 中所有索引定义。

---

## 已知问题与改进建议

| 方法 | 问题描述 | 建议改进 |
|------|---------|--------|
| `fieldsSQL` | 返回空字符串导致异常 | 应返回 `PRAGMA table_info('%s') % tablename` |
| `fkSQL` / `fkeys` | 未实现外键支持 | 可用 `PRAGMA foreign_key_list(tablename)` 实现 |
| `pkSQL` | 未实现 | 可删除或补充逻辑 |
| 类型解析正则 | 支持 `(len,dec)` 格式，但 SQLite 不常用 | 可简化或增强兼容性 |

---

## 示例用法（伪代码）

```python
db = SQLite3or(connection)
if db.isMe('sqlite3'):
    tables = db.tables()
    for t in tables:
        print(f"Table: {t['name']}")
        fields = db.fields(t['name'])
        for f in fields:
            print(f"  Field: {f['name']} ({f['type']})")
```

---

## 总结

`SQLite3or` 提供了一个轻量级的 SQLite 元数据访问框架，适合集成到动态 ORM 或低代码平台中。尽管部分功能尚未完成，但核心的字段映射、分页、表枚举已具备良好结构，可通过完善 SQL 查询语句进一步提升实用性。

--- 

📌 **维护建议**：优先修复 `fieldsSQL()` 和补充 `PRAGMA` 相关调用以确保元数据读取准确。