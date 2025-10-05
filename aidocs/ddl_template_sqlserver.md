# SQL Server DDL 模板技术文档（Jinja2 模板）

---

## 简介

`sqlserver_ddl_tmpl` 是一个基于 **Jinja2 模板引擎** 的 SQL Server 数据定义语言（DDL）生成模板。该模板用于根据元数据动态生成创建表、索引以及添加字段描述的完整 T-SQL 脚本。

支持功能包括：
- 字段类型映射
- 主键定义
- 唯一/普通索引创建
- 使用 `sp_addextendedproperty` 添加中文注释（MS_Description）
- 自动删除旧表（可选）

---

## 模板结构概览

```jinja2
{% macro typeStr(type, len, dec) %}...{% endmacro %}
{% macro nullStr(nullable) %}...{% endmacro %}
{% macro primary() %}...{% endmacro %}

drop table dbo.{{summary[0].name}};
CREATE TABLE dbo.{{summary[0].name}}
(
  ...
)
-- 创建索引
-- 添加表和列的描述信息
```

---

## 宏（Macros）说明

### `typeStr(type, len, dec)`

将通用字段类型转换为 SQL Server 对应的数据类型。

| 输入参数 | 类型 | 说明 |
|---------|------|------|
| `type`  | string | 字段逻辑类型（如 `'str'`, `'int'` 等） |
| `len`   | int    | 长度或精度（适用于字符串、数值等） |
| `dec`   | int    | 小数位数（适用于浮点数） |

#### 映射规则：

| 逻辑类型      | SQL Server 类型             | 说明 |
|---------------|------------------------------|------|
| `str`         | `NVARCHAR(len)`              | 变长 Unicode 字符串 |
| `char`        | `CHAR(len)`                  | 定长字符 |
| `long/int/short` | `NUMERIC`                 | 整数类型统一用 NUMERIC(无精度限制) |
| `float/double/ddouble` | `numeric(len, dec)` | 精确数值类型，支持小数 |
| `date/time`   | `DATE`                       | 日期类型 |
| `timestamp`   | `TIMESTAMP`                  | 时间戳（行版本号），注意：非 datetime 类型 |
| `text`        | `NVARCHAR(MAX)`              | 大文本字段 |
| `bin`         | `IMAGE`                      | 二进制大对象（已弃用，建议使用 `VARBINARY(MAX)`） |
| 其他未知类型  | 原样输出 `{{type}}`          | 扩展兼容性 |

> ⚠️ 注意：SQL Server 的 `TIMESTAMP` 实际是 **rowversion** 类型，若需时间记录请使用 `DATETIME2`。

---

### `nullStr(nullable)`

控制字段是否允许为空。

| 参数       | 值示例     | 输出结果       |
|------------|-----------|----------------|
| `nullable` | `'no'`     | `NOT NULL`     |
| 其他值（如 `'yes'`, 空等） | —— | （空字符串，即允许 NULL） |

---

### `primary()`

生成主键约束语句。

- 使用 `summary[0].primary` 中定义的字段列表。
- 输出格式：`, primary key (col1, col2, ...)`
- 仅在存在主键字段时插入。

---

## 主体 DDL 逻辑

### 1. 删除原表

```sql
drop table dbo.{{summary[0].name}};
```

> ⚠️ 此操作会 **无条件删除现有表及其数据**，生产环境慎用。

---

### 2. 创建表

```sql
CREATE TABLE dbo.{{summary[0].name}}
(
  {% for field in fields %}
    {{field.name}} {{typeStr(field.type,field.length,field.dec)}} {{nullStr(field.nullable)}}
    {%- if not loop.last -%},{%- endif %}
  {% endfor %}
  {% if summary[0].primary and len(summary[0].primary)>0 %}
    {{primary()}}
  {% endif %}
)
```

#### 字段循环说明：

- 遍历 `fields` 列表中每个字段对象。
- 生成字段名 + 类型 + 是否非空。
- 使用 `{%- if not loop.last %},{% endif %}` 控制逗号不加在最后一行。

#### 主键添加条件：

- 当 `summary[0].primary` 存在且长度 > 0 时，调用 `primary()` 宏添加主键定义。

---

### 3. 创建索引

```jinja2
{% for v in indexes %}
CREATE {% if v.idxtype=='unique' %}UNIQUE{% endif %} INDEX {{summary[0].name}}_{{v.name}} 
ON {{summary[0].name}}({{",".join(v.idxfields)}});
{% endfor %}
```

#### 支持索引类型：

| `v.idxtype` | 生成语句 |
|-------------|----------|
| `'unique'`  | `CREATE UNIQUE INDEX ...` |
| 其他（如 `'normal'`, 空值等） | `CREATE INDEX ...` |

#### 索引命名规则：

```
<表名>_<索引名>
```

例如：`user_idx_email` 表示表 `user` 上名为 `idx_email` 的索引。

---

### 4. 添加表与字段描述（MS_Description）

利用系统存储过程 `sys.sp_addextendedproperty` 添加注释。

#### 表级描述：

```sql
EXEC sys.sp_addextendedproperty 
  @name=N'MS_Description', 
  @value=N'{{summary[0].title}}',
  @level0type=N'SCHEMA', @level0name=N'dbo',
  @level1type=N'TABLE', @level1name=N'{{summary[0].name}}'
```

#### 字段级描述（逐个循环）：

```sql
{% for field in fields %}
EXEC sys.sp_addextendedproperty 
  @name=N'MS_Description', 
  @value=N'{{field.title}}',
  @level0type=N'SCHEMA', @level0name=N'dbo',
  @level1type=N'TABLE', @level1name=N'{{summary[0].name}}',
  @level2type=N'COLUMN', @level2name=N'{{field.name}}'
{% endfor %}
```

> ✅ 这些描述可在 SSMS 的“属性”面板或通过查询 `sys.extended_properties` 查看。

---

## 输入变量要求（上下文 Context）

模板运行需要以下变量注入：

| 变量名       | 类型       | 必须 | 说明 |
|-------------|------------|------|------|
| `summary`   | list[dict] | 是   | 表基本信息列表，通常只取 `[0]` |
| `fields`    | list[dict] | 是   | 字段列表 |
| `indexes`   | list[dict] | 否   | 索引配置列表（可为空） |

### `summary[0]` 结构示例：

```python
{
  "name": "user_info",           # 表名
  "title": "用户基本信息表"       # 表中文名/描述
  "primary": ["id"]              # 主键字段数组
}
```

### `fields` 元素结构示例：

```python
[
  {
    "name": "id",
    "type": "long",
    "length": 19,
    "dec": 0,
    "nullable": "no",
    "title": "用户ID"
  },
  {
    "name": "username",
    "type": "str",
    "length": 50,
    "dec": 0,
    "nullable": "no",
    "title": "用户名"
  }
]
```

### `indexes` 元素结构示例：

```python
[
  {
    "name": "idx_email",
    "idxtype": "unique",
    "idxfields": ["email"]
  },
  {
    "name": "idx_dept",
    "idxtype": "normal",
    "idxfields": ["dept_id", "status"]
  }
]
```

---

## 示例输出（渲染后）

假设输入如下元数据：

```python
summary = [{
    "name": "user",
    "title": "用户表",
    "primary": ["id"]
}]
fields = [
    {"name": "id", "type": "long", "length": 19, "dec": 0, "nullable": "no", "title": "用户ID"},
    {"name": "name", "type": "str", "length": 100, "dec": 0, "nullable": "yes", "title": "姓名"},
    {"name": "email", "type": "str", "length": 255, "dec": 0, "nullable": "no", "title": "邮箱"}
]
indexes = [
    {"name": "uk_email", "idxtype": "unique", "idxfields": ["email"]}
]
```

### 渲染结果：

```sql
drop table dbo.user;
CREATE TABLE dbo.user
(
  id NUMERIC NOT NULL,
  name NVARCHAR(100),
  email NVARCHAR(255) NOT NULL,
  primary key(id)
)

CREATE UNIQUE INDEX user_uk_email ON user(email);

EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'用户表' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'user'

EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'用户ID' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'user', @level2type=N'COLUMN',@level2name=N'id'
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'姓名' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'user', @level2type=N'COLUMN',@level2name=N'name'
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'邮箱' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'TABLE',@level1name=N'user', @level2type=N'COLUMN',@level2name=N'email'
```

---

## 使用建议与注意事项

1. 🔐 **谨慎使用 `DROP TABLE`**  
   生产环境中应避免直接删除表。建议改为判断是否存在再操作，或提供开关控制。

2. 💡 推荐扩展：增加 `IF NOT EXISTS` 判断表是否存在：

   ```sql
   IF OBJECT_ID('dbo.{{summary[0].name}}', 'U') IS NOT NULL
       DROP TABLE dbo.{{summary[0].name}};
   ```

3. 🛠 替代 IMAGE 类型：  
   `IMAGE` 已被弃用，推荐替换为：
   ```jinja2
   {%- elif type=='bin' -%}
   VARBINARY(MAX)
   ```

4. 📅 时间戳类型澄清：  
   若需存储时间，请使用 `DATETIME2` 并修改模板中 `timestamp` 分支。

5. 🔍 支持更多类型：可根据项目需求扩展 `typeStr` 宏以支持 `xml`, `hierarchyid`, `geometry` 等高级类型。

6. 🧪 测试建议：  
   在自动化脚本中集成此模板前，务必进行单元测试验证类型映射正确性。

---

## 总结

本模板是一个高效、灵活的 SQL Server 表结构自动生成工具，适用于代码生成器、ETL 工具、模型同步系统等场景。结合元数据驱动设计，可大幅提升数据库开发效率并保证一致性。

✅ **优点**：
- 类型自动映射
- 支持注释与索引
- 结构清晰易维护

⚠️ **风险提示**：
- 包含 `DROP TABLE`，使用需谨慎
- `TIMESTAMP` 和 `IMAGE` 类型需按实际需求调整

--- 

📅 最后更新：2025-04-05  
📦 所属模块：数据建模 / DDL 自动生成