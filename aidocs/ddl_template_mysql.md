以下是为提供的 Jinja2 模板代码编写的 **Markdown 格式技术文档**，适用于数据库建模或自动化 DDL 生成场景。

---

# MySQL DDL 自动生成模板技术文档

## 概述

本模板是一个基于 [Jinja2](https://jinja.palletsprojects.com/) 的 SQL DDL（数据定义语言）生成模板，用于根据元数据自动生成 MySQL 表的 `CREATE TABLE` 和相关索引语句。支持字段类型映射、默认值、非空约束、主键、注释以及普通/唯一索引的创建。

该模板可用于数据建模工具、ETL 流程配置、数据库初始化脚本等自动化场景。

---

## 模板变量说明

模板依赖以下上下文变量：

| 变量名     | 类型       | 说明 |
|------------|------------|------|
| `summary`  | 列表       | 表结构摘要信息列表，通常只包含一个元素（当前表），如：`summary[0].name`, `summary[0].primary`, `summary[0].title` |
| `fields`   | 列表       | 字段列表，每个字段包含名称、类型、长度、小数位、是否可为空、默认值、标题（注释）等属性 |
| `indexes`  | 列表       | 索引定义列表，每个索引包含名称、类型（`index` 或 `unique`）、字段列表 |

### `summary[0]` 结构示例
```json
{
  "name": "user_info",
  "primary": ["id"],
  "title": "用户基本信息表"
}
```

### `field` 结构示例
```json
{
  "name": "age",
  "type": "int",
  "length": 11,
  "dec": 0,
  "nullable": "no",
  "default": "0",
  "title": "年龄"
}
```

### `index` 结构示例
```json
[
  {
    "name": "idx_email",
    "idxtype": "unique",
    "idxfields": ["email"]
  },
  {
    "name": "idx_status",
    "idxtype": "index",
    "idxfields": ["status", "create_time"]
  }
]
```

---

## 宏函数（Macros）

模板中定义了多个辅助宏函数，用于格式化 SQL 片段。

### `typeStr(type, len, dec)`
将高级字段类型转换为对应的 MySQL 数据类型。

| 输入类型（`type`） | 输出 MySQL 类型 |
|---------------------|------------------|
| `str`               | `VARCHAR(len)`   |
| `char`              | `CHAR(len)`      |
| `int`, `long`, `short` | `int`         |
| `long`（单独判断）   | `bigint`         |
| `float`, `double`, `ddouble` | `double(len, dec)` |
| `date`              | `date`           |
| `time`              | `time`           |
| `datetime`          | `datetime`       |
| `timestamp`         | `TIMESTAMP DEFAULT CURRENT_TIMESTAMP` |
| `text`              | `longtext`       |
| `bin`               | `longblob`       |
| 其他未知类型        | 原样输出 `{{type}}` |

> ⚠️ 注意：`long` 类型在第一个条件中被映射为 `int`，但在后续又被单独判断为 `bigint` —— 这可能导致逻辑冲突，见下方【注意事项】。

### `defaultValue(defaultv)`
生成字段的默认值子句。

- 若 `defaultv` 存在，则输出：`DEFAULT 'value'`
- 若为空，则不输出任何内容

> 所有默认值均以字符串形式包裹（使用单引号），适合文本和数字；时间类型需确保传入格式正确。

### `nullStr(nullable)`
控制字段是否允许为空。

- 若 `nullable == 'no'` → 输出 `NOT NULL`
- 否则不输出

> 推荐约定：`yes/no` 表示可空性，也可扩展为布尔值处理。

### `primary()`
生成主键定义语句。

- 使用 `summary[0].primary` 中的字段名列表，用逗号连接
- 输出：`, primary key (col1,col2,...)`

> 注意：此宏前会自动换行并缩进，需配合字段列表使用。

---

## 生成的 SQL 内容结构

### 1. 删除已有表
```sql
DROP TABLE IF EXISTS {{summary[0].name}};
```

### 2. 创建新表
```sql
CREATE TABLE table_name (
  `field1` type ... [NOT NULL] [DEFAULT '...'] [COMMENT '...'],
  ...
  [PRIMARY KEY (pk_fields)]
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8
[COMMENT='表注释'];
```

### 3. 创建索引
遍历 `indexes` 数组，生成：
```sql
CREATE [UNIQUE] INDEX index_name ON table_name(field1, field2);
```

---

## 示例输出

假设输入如下元数据：

```python
summary = [{
    "name": "user",
    "primary": ["id"],
    "title": "用户表"
}]

fields = [
    {
        "name": "id",
        "type": "long",
        "length": 20,
        "dec": 0,
        "nullable": "no",
        "default": "",
        "title": "用户ID"
    },
    {
        "name": "username",
        "type": "str",
        "length": 50,
        "dec": 0,
        "nullable": "no",
        "default": "",
        "title": "用户名"
    },
    {
        "name": "created_at",
        "type": "timestamp",
        "length": 0,
        "dec": 0,
        "nullable": "yes",
        "default": "",
        "title": "创建时间"
    }
]

indexes = [
    {
        "name": "uk_username",
        "idxtype": "unique",
        "idxfields": ["username"]
    }
]
```

### 生成的 SQL：
```sql
DROP TABLE IF EXISTS user;
CREATE TABLE user
(
  `id` bigint NOT NULL,
  `username` VARCHAR(50) NOT NULL,
  `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP 
)
engine=innodb 
default charset=utf8 
comment '用户表';

CREATE UNIQUE INDEX user_uk_username ON user(username);
```

---

## 注意事项与建议

1. ❗ **类型判断逻辑问题**：
   ```jinja2
   {% elif type=='long' or type=='int' or type=='short' %}
   int
   {% elif type=='long' %}
   bigint
   ```
   上述逻辑中，`long` 已在第一个分支被捕获为 `int`，因此永远不会进入 `bigint` 分支！  
   ✅ **修复建议**：
   ```jinja2
   {%- if type == 'long' -%}
   bigint
   {%- elif type in ['int', 'short'] -%}
   int
   ```

2. 🔤 所有字符串默认值都加了单引号，若需支持数值型默认值（如 `DEFAULT 0`），应做类型区分。

3. 🧩 `summary` 设计为列表形式，但实际仅使用 `summary[0]`，可考虑简化为直接对象传参。

4. 💬 注释建议统一使用 UTF-8 编码，避免中文乱码。

5. 🔐 生产环境建议增加 SQL 注入风险校验（如字段名、表名合法性验证）。

---

## 使用方式（Python 示例）

```python
from jinja2 import Template

# 加载模板
tpl = Template(mysql_ddl_tmpl)

# 渲染 SQL
sql = tpl.render(summary=summary, fields=fields, indexes=indexes)

print(sql)
```

---

## 版本历史

| 版本 | 日期       | 描述 |
|------|------------|------|
| 1.0  | 2025-04    | 初始版本，支持基础字段类型与索引生成 |

---

## 许可与维护

- **作者**：Auto-generated
- **用途**：内部系统 / 数据仓库建模
- **维护建议**：结合 Schema JSON 配置驱动模板渲染，提升可维护性。

--- 

✅ 文档结束