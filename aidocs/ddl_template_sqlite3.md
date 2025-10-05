以下是为提供的 Jinja2 模板代码编写的 **Markdown 格式技术文档**，适用于开发人员理解该模板的功能、结构和使用方式。

---

# SQLite3 DDL 生成模板技术文档

`sqlite3_ddl_tmpl` 是一个基于 [Jinja2](https://jinja.palletsprojects.com/) 的模板字符串，用于动态生成符合 SQLite3 语法的表定义（DDL）语句。它支持字段类型映射、空值约束、主键定义、注释以及索引创建。

## 目录

- [功能概述](#功能概述)
- [模板变量说明](#模板变量说明)
- [宏（Macros）详解](#宏macros详解)
  - [`typeStr(type, len, dec)`](#typestrtype-len-dec)
  - [`nullStr(nullable)`](#nullstrnullable)
  - [`primary()`](#primary)
- [生成的 DDL 结构](#生成的-ddl-结构)
- [输出示例](#输出示例)
- [使用场景](#使用场景)
- [注意事项](#注意事项)

---

## 功能概述

此模板用于根据元数据自动生成如下内容：

1. 删除已存在的同名表（`DROP TABLE IF EXISTS`）
2. 创建新表（`CREATE TABLE`），包括：
   - 字段名、类型、是否非空
   - 字段级注释（通过 `--` 注释实现）
   - 主键定义
3. 可选的唯一或普通索引创建（`CREATE INDEX` / `CREATE UNIQUE INDEX`）

所有逻辑均通过 Jinja2 模板语言实现，适合作为代码生成器的一部分，集成在 ORM 工具或数据库建模系统中。

---

## 模板变量说明

| 变量 | 类型 | 描述 |
|------|------|------|
| `summary[0]` | dict/object | 表的摘要信息对象，包含：`name`（表名）、`primary`（主键字段列表）、`title`（表标题/描述） |
| `fields` | list | 字段列表，每个字段对象包含：<br>• `name`: 字段名<br>• `type`: 数据类型（如 `'str'`, `'int'` 等）<br>• `length`: 长度（可选）<br>• `dec`: 小数位数（可选）<br>• `nullable`: 是否允许为空（`'yes'` 或 `'no'`）<br>• `title`: 字段说明（作为注释显示） |
| `indexes` | list | 索引列表，每个索引对象包含：<br>• `name`: 索引名称<br>• `idxtype`: 索引类型（`'unique'` 或其他）<br>• `idxfields`: 构成索引的字段名列表 |

---

## 宏（Macros）详解

### `typeStr(type, len, dec)`

将高级数据类型转换为 SQLite 原生类型。

#### 参数：
- `type`: 字符串，表示原始类型（如 `'str'`, `'int'`, `'float'` 等）
- `len`: 字段长度（未使用于 SQLite 类型推断，仅保留接口兼容性）
- `dec`: 小数位数（同上）

#### 映射规则：

| 输入类型 | 输出 SQLite 类型 |
|---------|----------------|
| `str`, `char`, `date`, `time`, `datetime`, `timestamp` | `TEXT` |
| `long`, `int`, `short`, `longlong` | `INTEGER`（模板中写作 `int`） |
| `float`, `double`, `ddouble` | `REAL` |
| `bin` | `BLOB` |
| 其他未知类型 | 原样输出 `{{type}}` |

> ⚠️ 注意：SQLite 实际使用的是“类型亲和性”（Type Affinity），但此处简化处理以增强可读性和一致性。

---

### `nullStr(nullable)`

生成 `NOT NULL` 约束条件。

#### 参数：
- `nullable`: `'no'` 表示不允许为空；其他值（如 `'yes'`）则不添加约束

#### 输出：
- 若 `nullable == 'no'` → 输出 `NOT NULL`
- 否则输出空字符串

---

### `primary()`

生成主键子句。

#### 逻辑：
- 使用 `summary[0].primary` 中的字段名列表，用逗号连接
- 添加 `, primary key(...)` 子句

#### 示例：
```sql
,primary key(id, code)
```

> ✅ 仅当存在主键且字段数 > 0 时才插入该行。

---

## 生成的 DDL 结构

模板最终输出的标准 SQL 包括以下部分：

```sql
DROP TABLE IF EXISTS <table_name>;

CREATE TABLE <table_name>
(
  `field1` <type> <NOT NULL?> -- 字段说明
  ...
  ,primary key(pk_field1, pk_field2)
) -- 表说明 ;

CREATE INDEX|UNIQUE INDEX index_name ON table_name(field_list);
...
```

每条语句严格按照 SQLite 语法编写，并带有良好格式化与注释支持。

---

## 输出示例

假设输入数据如下：

```python
summary = [{
    "name": "users",
    "primary": ["id"],
    "title": "用户信息表"
}]

fields = [
    {"name": "id", "type": "int", "nullable": "no", "title": "用户ID"},
    {"name": "name", "type": "str", "length": 50, "nullable": "no", "title": "用户名"},
    {"name": "email", "type": "str", "length": 100, "nullable": "yes", "title": "邮箱地址"},
    {"name": "created_at", "type": "datetime", "nullable": "no", "title": "创建时间"}
]

indexes = [
    {"name": "email_idx", "idxtype": "", "idxfields": ["email"]},
    {"name": "uniq_name", "idxtype": "unique", "idxfields": ["name"]}
]
```

### 生成的 SQL：

```sql
drop table if exists users;
CREATE TABLE users
(
  `id` int NOT NULL  -- 用户ID,
  `name` TEXT NOT NULL  -- 用户名,
  `email` TEXT  -- 邮箱地址,
  `created_at` TEXT NOT NULL  -- 创建时间
,primary key(id)
) --用户信息表
;

CREATE INDEX users_email_idx ON users(email);
CREATE UNIQUE INDEX users_uniq_name ON users(name);
```

---

## 使用场景

该模板适用于以下场景：

- 自动化数据库建模工具
- 数据字典到物理模型的转换
- 跨平台迁移时的目标 DDL 生成（特别是轻量级 SQLite 场景）
- 内嵌式应用初始化脚本生成

结合 Python 的 `jinja2.Template(sqlite3_ddl_tmpl).render(...)` 即可快速渲染出实际 SQL。

---

## 注意事项

1. **字段名使用反引号包裹**：确保特殊字符或关键字字段的安全性。
2. **SQLite 类型限制**：
   - 实际存储依赖“动态类型”，但建议保持类型亲和性一致。
   - 没有原生 `DATETIME` 类型，使用 `TEXT` 存储 ISO8601 时间字符串。
3. **主键语法位置**：主键定义位于字段之后，以逗号开头拼接，需注意前面是否有字段结尾的逗号。
4. **索引命名规范**：采用 `<table>_<index_name>` 形式避免冲突。
5. **注释支持**：利用 `--` 添加人类可读说明，不影响执行。

---

## 版权与维护

- **作者**：Auto-generated Template
- **用途**：内部代码生成组件
- **建议扩展**：可根据需要增加默认值、外键等支持。

--- 

✅ 文档版本：v1.0  
📅 最后更新：2025-04-05