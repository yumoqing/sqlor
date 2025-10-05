以下是为提供的 Jinja2 模板代码编写的 **Markdown 格式技术文档**，用于描述该 PostgreSQL DDL 生成模板的功能、结构和使用方式。

---

# PostgreSQL DDL 生成模板技术文档

## 概述

`postgresql_ddl_tmpl` 是一个基于 **Jinja2 模板引擎**的字符串模板，用于自动生成符合 PostgreSQL 语法的 **数据定义语言 (DDL)** 脚本。它可以根据表结构元数据（如字段类型、长度、是否可为空、主键、索引等）动态生成完整的 `CREATE TABLE` 语句，包括：

- 表的创建与删除
- 字段定义及其数据类型映射
- 主键约束
- 索引（普通或唯一）
- 表和字段的注释

该模板适用于自动化建模工具、ETL 流程或数据库同步系统中，实现从元数据到物理表结构的自动转换。

---

## 模板源码

```python
postgresql_ddl_tmpl = """{% macro typeStr(type,len,dec) %}
{%- if type=='str' -%}
VARCHAR({{len}})
{%- elif type=='char' -%}
CHAR({{len}})
{%- elif type=='long' or type=='int' or type=='short' -%}
NUMERIC(30,0)
{%- elif type=='float' or type=='double' or type=='ddouble' -%}
NUMERIC({{len}},{{dec}})
{%- elif type=='date' -%}
DATE
{%- elif type=='time' -%}
TIME
{%- elif type=='timestamp' -%}
TIMESTAMP
{%- else -%}
{{type}}
{%- endif %}
{%- endmacro %}
{% macro nullStr(nullable) %}
{%- if nullable=='no' -%}
NOT NULL
{%- endif -%}
{% endmacro %}
{% macro primary() %}
,PRIMARY KEY({{','.join(summary[0].primary)}})
{% endmacro %}
DROP TABLE IF EXISTS {{summary[0].name}};
CREATE TABLE {{summary[0].name}}
(
{% for field in fields %}
  {{field.name}} {{typeStr(field.type,field.length,field.dec)}} {{nullStr(field.nullable)}}{%- if not loop.last -%},{%- endif -%}
{% endfor %}
{% if summary[0].primary and len(summary[0].primary)>0 %}
{{primary()}}
{% endif %}
);
{% for v in indexes %}
CREATE {% if v.idxtype=='unique' %}UNIQUE{% endif %} INDEX {{summary[0].name}}_{{v.name}} ON {{summary[0].name}}({{",".join(v.idxfields)}});
{%- endfor -%}
COMMENT ON TABLE {{summary[0].name}} IS '{{summary[0].title}}';
{% for field in fields %}
COMMENT ON COLUMN {{summary[0].name}}.{{field.name}} is '{{field.title}}';
{% endfor %}
"""
```

---

## 模板变量说明

| 变量名 | 类型 | 描述 |
|-------|------|------|
| `summary[0]` | 对象 | 包含表级别的元信息对象，必须是列表且第一个元素有效 |
| &nbsp;&nbsp;`.name` | 字符串 | 表名（如 `"users"`） |
| &nbsp;&nbsp;`.title` | 字符串 | 表的中文/描述性标题，用于 COMMENT |
| &nbsp;&nbsp;`.primary` | 列表 | 主键字段名称列表（如 `["id"]` 或 `["user_id", "tenant_id"]`） |
| `fields` | 列表 | 字段对象列表，每个对象代表一列 |
| &nbsp;&nbsp;`.name` | 字符串 | 字段名（如 `"username"`） |
| &nbsp;&nbsp;`.type` | 字符串 | 字段逻辑类型（见下文“类型映射”） |
| &nbsp;&nbsp;`.length` | 整数 | 字段长度（对字符串和数值类型有意义） |
| &nbsp;&nbsp;`.dec` | 整数 | 小数位数（decimal scale） |
| &nbsp;&nbsp;`.nullable` | 字符串 | 是否允许为空：`"yes"` 表示可空，`"no"` 表示 `NOT NULL` |
| &nbsp;&nbsp;`.title` | 字符串 | 字段的中文/描述性标题，用于 COMMENT |
| `indexes` | 列表 | 索引定义对象列表 |
| &nbsp;&nbsp;`.name` | 字符串 | 索引名称标识（将用于生成索引名） |
| &nbsp;&nbsp;`.idxtype` | 字符串 | 索引类型，若为 `"unique"` 则创建唯一索引 |
| &nbsp;&nbsp;`.idxfields` | 列表 | 索引包含的字段名列表（如 `["email"]` 或 `["status", "created_time"]`） |

---

## 内置宏（Macros）

### `typeStr(type, len, dec)`
根据字段的逻辑类型映射为 PostgreSQL 实际数据类型。

| 逻辑类型 | 映射结果 |
|---------|--------|
| `'str'` | `VARCHAR(len)` |
| `'char'` | `CHAR(len)` |
| `'long'`, `'int'`, `'short'` | `NUMERIC(30,0)` （高精度整数） |
| `'float'`, `'double'`, `'ddouble'` | `NUMERIC(len, dec)` |
| `'date'` | `DATE` |
| `'time'` | `TIME` |
| `'timestamp'` | `TIMESTAMP` |
| 其他未知类型 | 原样输出 `{{type}}`（可用于扩展） |

> ⚠️ 注意：浮点类型统一使用 `NUMERIC` 以保证精度；如需性能优化可后续调整为 `REAL` / `DOUBLE PRECISION`。

---

### `nullStr(nullable)`
生成 `NOT NULL` 约束。

- 若 `nullable == 'no'` → 输出 `NOT NULL`
- 否则不输出任何内容（即允许为空）

---

### `primary()`
生成主键约束子句。

- 使用 `summary[0].primary` 中的字段名，通过逗号拼接
- 示例：主键为 `["id"]` → 输出 `,PRIMARY KEY(id)`
- 注意：前面有一个英文逗号，依赖前一行结尾，因此必须确保前面有换行和逗号处理

---

## 生成的 DDL 结构说明

1. **删除旧表**
   ```sql
   DROP TABLE IF EXISTS table_name;
   ```
   > 避免重复建表错误。

2. **创建新表**
   ```sql
   CREATE TABLE table_name (...);
   ```
   - 包含所有字段定义
   - 自动添加主键约束（如果存在）
   - 支持多字段联合主键

3. **创建索引**
   ```sql
   CREATE [UNIQUE] INDEX table_name_idxname ON table_name(field1, field2);
   ```
   - 支持普通索引和唯一索引
   - 索引命名规则：`{table_name}_{index_name}`

4. **添加注释**
   ```sql
   COMMENT ON TABLE table_name IS '用户信息表';
   COMMENT ON COLUMN table_name.username IS '用户名';
   ```
   - 提升数据库可读性和文档性

---

## 使用示例

假设输入上下文如下（Python 字典形式）：

```python
context = {
    "summary": [{
        "name": "users",
        "title": "用户信息表",
        "primary": ["id"]
    }],
    "fields": [
        {"name": "id", "type": "long", "length": 30, "dec": 0, "nullable": "no", "title": "用户ID"},
        {"name": "username", "type": "str", "length": 50, "dec": 0, "nullable": "no", "title": "用户名"},
        {"name": "email", "type": "str", "length": 100, "dec": 0, "nullable": "yes", "title": "邮箱地址"},
        {"name": "created_time", "type": "timestamp", "length": 0, "dec": 0, "nullable": "no", "title": "创建时间"}
    ],
    "indexes": [
        {"name": "email_idx", "idxtype": "unique", "idxfields": ["email"]}
    ]
}
```

渲染后输出 SQL：

```sql
DROP TABLE IF EXISTS users;
CREATE TABLE users
(
  id NUMERIC(30,0) NOT NULL,
  username VARCHAR(50) NOT NULL,
  email VARCHAR(100),
  created_time TIMESTAMP NOT NULL,
  PRIMARY KEY(id)
);

CREATE UNIQUE INDEX users_email_idx ON users(email);

COMMENT ON TABLE users IS '用户信息表';
COMMENT ON COLUMN users.id is '用户ID';
COMMENT ON COLUMN users.username is '用户名';
COMMENT ON COLUMN users.email is '邮箱地址';
COMMENT ON COLUMN users.created_time is '创建时间';
```

---

## 扩展建议

- ✅ **支持默认值**：可在字段中增加 `.default` 属性，并在模板中扩展。
- ✅ **外键支持**：当前未处理外键，可根据需要添加 `FOREIGN KEY` 子句。
- ✅ **字符集/排序规则**：PostgreSQL 通常使用数据库级设置，也可显式指定。
- ✅ **分区表支持**：复杂场景下可扩展模板支持 `PARTITION BY`。

---

## 注意事项

1. `primary()` 宏前会自动加 `,`，因此必须确保前面字段定义已正确结束并换行。
2. 所有字段的 `length` 和 `dec` 必须提供，即使某些类型不需要（可用占位值如 `0`）。
3. `summary` 必须是一个非空列表，`summary[0]` 是实际使用的表元数据。
4. 索引名应避免冲突，推荐命名清晰（如 `idx_status`）。

---

## 总结

`postgresql_ddl_tmpl` 是一个功能完整、结构清晰的 DDL 自动生成模板，适合集成进元数据驱动的数据平台。其优势在于：

- 类型抽象良好，便于业务人员理解
- 支持常见约束与注释
- 易于维护和扩展

通过此模板，可以高效地将模型设计转化为可执行的数据库脚本，提升开发效率与一致性。

--- 

✅ *文档版本：1.0*  
📅 *最后更新：2025年4月5日*