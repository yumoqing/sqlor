以下是为提供的 Jinja2 模板代码编写的 **Markdown 格式技术文档**，适用于 Oracle 数据库 DDL 生成场景。

---

# Oracle DDL 模板技术文档

## 概述

`oracle_ddl_tmpl` 是一个基于 [Jinja2](https://jinja.palletsprojects.com/) 的模板字符串，用于自动生成 Oracle 数据库表的 **数据定义语言 (DDL)** 脚本。该模板支持字段类型映射、主键定义、索引创建以及注释添加等功能，适用于自动化建模或元数据驱动的数据库构建流程。

---

## 使用场景

- 自动化生成 Oracle 表结构（`CREATE TABLE`）
- 支持字段级和表级中文注释
- 支持主键、唯一/普通索引定义
- 可集成至 ETL 工具、数据建模平台或 CI/CD 流程中

---

## 模板变量说明

### 输入上下文参数

| 参数名 | 类型 | 说明 |
|--------|------|------|
| `summary[0]` | 对象 | 表元数据对象，包含表名、标题、主键等信息 |
| &nbsp;&nbsp;`.name` | 字符串 | 表名（如：`EMPLOYEE`） |
| &nbsp;&nbsp;`.title` | 字符串 | 表中文描述（用于 COMMENT） |
| &nbsp;&nbsp;`.primary` | 列表 | 主键字段名称列表（如：`['ID']`） |
| `fields` | 列表 | 字段对象列表，每个字段包含属性如名称、类型、长度等 |
| `indexes` | 列表 | 索引对象列表，定义索引类型与字段 |

#### `fields` 中单个字段对象结构

| 属性 | 类型 | 说明 |
|------|------|------|
| `name` | 字符串 | 字段英文名（如：`EMP_NAME`） |
| `type` | 字符串 | 字段逻辑类型（见下文映射规则） |
| `length` | 整数 | 字段长度（对数值/字符类型有效） |
| `dec` | 整数 | 小数位数（仅对浮点类型有效） |
| `nullable` | 字符串 | 是否可为空：`yes` / `no` |
| `title` | 字符串 | 字段中文描述（用于 COMMENT） |

#### `indexes` 中单个索引对象结构

| 属性 | 类型 | 说明 |
|------|------|------|
| `name` | 字符串 | 索引标识名（将用于生成索引名） |
| `idxtype` | 字符串 | 索引类型：`unique` 或其他（非 unique 视为普通索引） |
| `idxfields` | 列表 | 索引包含的字段名列表（如：`['DEPT_ID', 'STATUS']`） |

---

## 模板功能详解

### 宏定义（Macros）

#### `typeStr(type, len, dec)`
根据字段逻辑类型映射为 Oracle 原生数据类型。

| 逻辑类型 | Oracle 实际类型 | 示例输出 |
|----------|------------------|-----------|
| `str`     | `VARCHAR2(len)`   | `VARCHAR2(50)` |
| `char`    | `CHAR(len)`       | `CHAR(10)` |
| `long`, `int`, `short` | `NUMBER` | `NUMBER` |
| `float`, `double`, `ddouble` | `NUMBER(len, dec)` | `NUMBER(10, 2)` |
| `date`, `time` | `DATE` | `DATE` |
| `timestamp` | `TIMESTAMP` | `TIMESTAMP` |
| `text`      | `CLOB` | `CLOB` |
| `bin`       | `BLOB` | `BLOB` |
| 其他未知类型 | 原样输出 `{{type}}` | 如：`XMLTYPE` |

> ⚠️ 注意：`len` 和 `dec` 需在输入数据中提供，否则可能导致渲染错误。

#### `nullStr(nullable)`
判断字段是否非空。

- 若 `nullable == 'no'` → 输出 `NOT NULL`
- 否则不输出任何内容（即允许为空）

#### `primary()`
生成主键约束子句。

- 使用 `summary[0].primary` 列表中的字段名拼接
- 输出格式：`, primary key (col1,col2,...)`

> ✅ 提示：此宏前需确保已有字段定义，并以逗号分隔最后一个字段。

---

### 主体 DDL 语句结构

#### 1. 删除旧表
```sql
drop table {{summary[0].name}};
```
> ⚠️ 警告：无条件删除同名表，请谨慎使用于生产环境。

#### 2. 创建新表
```sql
CREATE TABLE {{summary[0].name}}
(
  -- 字段列表循环生成 --
  {% for field in fields %}
    {{field.name}} {{typeStr(...)}} {{nullStr(...)}}{%- if not loop.last %},{% endif %}
  {% endfor %}

  -- 主键定义（如有）--
  {% if summary[0].primary and len(summary[0].primary)>0 %}
    {{primary()}}
  {% endif %}
);
```

#### 3. 创建索引
```sql
{% for v in indexes %}
  CREATE {% if v.idxtype=='unique' %}UNIQUE{% endif %} INDEX 
    {{summary[0].name}}_{{v.name}} 
  ON {{summary[0].name}}({{",".join(v.idxfields)}});
{% endfor %}
```
- 自动生成命名规则：`表名_索引名`
- 支持唯一索引标记

#### 4. 添加注释
```sql
COMMENT ON TABLE {{summary[0].name}} IS '{{summary[0].title}}';

{% for field in fields %}
  COMMENT ON COLUMN {{summary[0].name}}.{{field.name}} IS '{{field.title}}';
{% endfor %}
```
> ✅ 支持中文注释，提升数据库可读性与维护性。

---

## 示例输出（片段）

假设输入如下：
```python
summary = [{
    "name": "EMPLOYEE",
    "title": "员工信息表",
    "primary": ["EMP_ID"]
}]
fields = [
    {"name": "EMP_ID", "type": "long", "nullable": "no", "title": "员工ID"},
    {"name": "EMP_NAME", "type": "str", "length": 100, "nullable": "no", "title": "姓名"},
    {"name": "SALARY", "type": "double", "length": 12, "dec": 2, "nullable": "yes", "title": "薪资"}
]
indexes = [
    {"name": "IDX_NAME", "idxtype": "unique", "idxfields": ["EMP_NAME"]}
]
```

生成 SQL 片段：
```sql
drop table EMPLOYEE;
CREATE TABLE EMPLOYEE
(
  EMP_ID NUMBER NOT NULL,
  EMP_NAME VARCHAR2(100) NOT NULL,
  SALARY NUMBER(12,2)
,primary key(EMP_ID)
);

CREATE UNIQUE INDEX EMPLOYEE_IDX_NAME ON EMPLOYEE(EMP_NAME);

COMMENT ON TABLE EMPLOYEE IS '员工信息表';
COMMENT ON COLUMN EMPLOYEE.EMP_ID is '员工ID';
COMMENT ON COLUMN EMPLOYEE.EMP_NAME is '姓名';
COMMENT ON COLUMN EMPLOYEE.SALARY is '薪资';
```

---

## 注意事项与最佳实践

1. **安全性警告**
   - `DROP TABLE` 会清除现有数据，请确认是否需要保留历史数据。
   - 建议在正式环境中禁用自动 drop，或增加条件判断。

2. **字段类型扩展**
   - 当前仅覆盖常见类型，如需支持 `XMLTYPE`、`INTERVAL` 等，可通过 `else` 分支直接传入原生类型。

3. **索引命名规范**
   - 推荐保持 `表名_索引名` 的简洁风格，避免过长或冲突。

4. **Jinja2 渲染要求**
   - 必须保证传入的数据结构完整，避免访问不存在的属性导致异常。
   - 建议进行前置校验（如字段必填项检查）。

5. **字符集与存储**
   - `VARCHAR2` 和 `CHAR` 默认按字节计算，若需按字符建议手动调整或通过外部配置处理。

6. **CLOB/BLOB 存储优化**
   - 大对象字段可能影响性能，建议结合业务需求设置合理的存储参数（当前模板未包含）。

---

## 扩展建议

- 添加 `IF NOT EXISTS` 判断（Oracle 不原生支持，可用 PL/SQL 包装）
- 支持外键约束生成
- 支持分区表语法
- 引入模板配置层，实现多数据库兼容（如 MySQL、PostgreSQL）

---

## 许可与维护

- **作者**：Auto-generated Template
- **用途**：内部系统自动化建模
- **依赖**：Python + Jinja2 >= 3.0
- **更新时间**：2025年4月5日

--- 

✅ 文档结束。可用于团队知识共享、项目文档归档或工具集成说明。