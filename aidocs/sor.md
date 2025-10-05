# SQLor 技术文档

`SQLor` 是一个用于简化数据库操作的异步 Python 数据库访问类，支持参数化 SQL 执行、分页查询、CRUD 操作（Create, Read, Update, Delete）、元数据获取、动态 SQL 构建等功能。它设计为可扩展，适用于多种数据库后端。

---

## 目录

1. [概述](#概述)
2. [依赖与环境要求](#依赖与环境要求)
3. [核心功能](#核心功能)
4. [主要类与方法说明](#主要类与方法说明)
   - [`db_type_2_py_type`](#db_type_2_py_type)
   - [`SQLorException`](#sqlorexception)
   - [`findNamedParameters`](#findnamedparameters)
   - [`uniParams`](#uniparams)
   - [`readsql`](#readsql)
   - [`SQLor` 类](#sqlor-类)
     - 初始化
     - 元数据管理
     - 连接与游标控制
     - SQL 处理与转换
     - 查询执行
     - 分页与排序
     - 聚合操作（Pivot）
     - 表结构信息获取
     - DDL 与表定义操作
     - CRUD 操作（C/U/R/D）
5. [使用示例](#使用示例)
6. [异常处理](#异常处理)
7. [配置说明](#配置说明)

---

## 概述

`SQLor` 提供了一个统一接口来执行 SQL 查询和命令，并通过命名参数 `${var}` 支持模板化 SQL。其主要特点包括：

- 异步/同步双模式支持。
- 参数自动替换与安全绑定。
- 自动识别 `SELECT`, `INSERT/UPDATE/DELETE`, `DDL` 类型并返回相应结果。
- 支持分页、排序、条件过滤。
- 内置对表结构（字段、主键、外键、索引）的元数据查询。
- 支持自定义函数注册钩子（before/after 钩子）。
- 可扩展的类型转换机制。
- 简化的 CRUD 接口（I/C/R/U/D）。

---

## 依赖与环境要求

### 第三方依赖

```python
from appPublic.myImport import myImport
from appPublic.dictObject import DictObject
from appPublic.unicoding import uDict
from appPublic.myTE import MyTemplateEngine
from appPublic.objectAction import ObjectAction
from appPublic.argsConvert import ArgsConvert, ConditionConvert
from appPublic.registerfunction import RegisterFunction
from appPublic.log import info, exception, debug
```

> ⚠️ 注意：这些模块属于 `appPublic` 包，需确保已安装或项目中包含该包。

### Python 内置模块

- `os`
- `sys`
- `decimal`
- `datetime`
- `codecs`
- `re`
- `json`
- `traceback`

### 环境变量

```python
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
```

> 仅在 Oracle 数据库环境下需要设置 NLS 编码以支持中文 UTF-8。

---

## 核心功能

| 功能 | 描述 |
|------|------|
| 参数化 SQL | 使用 `${var}` 占位符进行变量注入 |
| 异步支持 | 支持 `async/await` 模式运行 SQL |
| 分页查询 | 自动生成分页 SQL（需子类实现模型） |
| 排序与过滤 | 支持动态排序字段和条件表达式 |
| 元数据提取 | 获取表、字段、主键、索引等结构信息 |
| CRUD 封装 | 提供 `C()`(创建), `R()`(读取), `U()`(更新), `D()`(删除) 方法 |
| 回调机制 | 支持逐行处理查询结果 |
| 类型转换 | 自动将数据库类型转为 Python 原生类型 |

---

## 主要类与方法说明

### `db_type_2_py_type(o)`

将特定数据库类型转换为 Python 原生类型。

#### 参数
- `o`: 数据库字段值

#### 返回值
| 输入类型 | 输出 |
|--------|-------|
| `decimal.Decimal` | `float` |
| `datetime.datetime` | 字符串格式 `'YYYY-MM-DD HH:MM:SS'` |
| `datetime.date` | `'YYYY-MM-DD'` |
| 其他 | 原值 |

#### 示例
```python
db_type_2_py_type(decimal.Decimal('3.14')) → 3.14
```

---

### `SQLorException`

自定义异常类，封装 SQL 执行错误。

> ❗ 当前代码存在拼写错误：`__int__` 应为 `__init__`，且 `supper` 应为 `super`。

#### 属性
- `response`: `"error"`
- `errtype`: `"SQLor"`
- `errmsg`: 异常消息字符串

#### 示例输出
```
errtype:SQLor,errmsg=database error...
```

> ✅ **建议修复**：
```python
class SQLException(Exception):
    def __init__(self, **kvs):
        super(SQLException, self).__init__(**kvs)
        self.dic = {
            'response': 'error',
            'errtype': 'SQLor',
            'errmsg': str(self)
        }

    def __str__(self):
        return 'errtype:%s,errmsg=%s' % (self.dic['errtype'], self.dic['errmsg'])
```

---

### `setValues(params, ns)`

从命名空间 `ns` 或系统环境变量中查找参数值。

#### 参数
- `params`: 参数名（字符串）
- `ns`: 命名空间字典

#### 返回
优先从 `ns` 中取值，若无则尝试 `os.getenv(params)`。

---

### `findNamedParameters(sql)`

解析 SQL 字符串中的所有 `${...}` 形式的命名参数。

#### 示例
```python
sql = "SELECT * FROM user WHERE id = ${uid}$ AND name = ${uname}$"
findNamedParameters(sql)
# 返回 ['${uid}$', '${uname}$']
```

---

### `uniParams(params1)`

去重保留唯一参数名列表。

#### 示例
```python
uniParams(['${a}$', '${b}$', '${a}$']) → ['${a}$', '${b}$']
```

---

### `readsql(fn)`

读取指定文件路径的 SQL 文件内容（UTF-8 编码）。

#### 参数
- `fn`: 文件路径

#### 返回
文件内容字符串。

---

## `SQLor` 类

### 初始化 `__init__`

#### 参数
| 参数 | 默认值 | 说明 |
|------|--------|------|
| `dbdesc` | None | 数据库描述字典，至少含 `dbname` |
| `sqltp`, `sqlts` | `$[`, `]$` | 模板占位符开始/结束符号 |
| `sqlvp`, `sqlvs` | `${`, `}$` | 变量占位符开始/结束符号 |

#### 初始化行为
- 设置连接状态（`conn`, `cur`）
- 初始化元数据缓存 `metadatas`
- 创建 `ConditionConvert` 实例用于条件表达式处理

---

### 元数据管理

#### `setMeta(tablename, meta)`
保存表的元数据到内存缓存。

#### `getMeta(tablename)`
获取指定表的缓存元数据。

#### `removeMeta(tablename)`
清除指定表的元数据缓存。

---

### 连接与游标控制

#### `setCursor(async_mode, conn, cur)`
绑定数据库连接和游标对象。

#### `getConn()`
返回当前数据库连接对象。

#### `cursor()`
返回当前游标对象。

---

### 类型转换支持

#### `setConvertFunction(typ, func)`
注册用户自定义类型转换函数。

#### `convert(typ, value)`
调用注册的转换函数处理值。

---

### SQL 类型判断

#### `getSqlType(sql)`
判断 SQL 语句类型。

| 类型 | 条件 |
|------|------|
| `"qry"` | 以 `SELECT` 开头 |
| `"dml"` | `INSERT`, `UPDATE`, `DELETE` |
| `"ddl"` | 其他（如 `CREATE`, `DROP`） |

---

### SQL 模板处理

#### `maskingSQL(org_sql, NS)`
将命名参数 `${var}$` 替换为 `?` 并生成参数列表。

##### 步骤
1. 先替换 `$[...]$` 模板片段（如条件块）
2. 提取 `${var}$` 参数名
3. 替换为 `?` 占位符
4. 返回 `(标记后的SQL, 参数值列表)`

##### 特殊变量
- `__mainsql__`: 不参与参数绑定，用于嵌套 SQL 注入

---

### 执行方法

#### `execute(sql, value, callback, **kwargs)`
执行单条 SQL，支持回调逐行处理结果。

##### 流程
- 调用 `runVarSQL` 执行 SQL
- 若是查询 (`qry`) 且有回调，则逐行调用 `callback(DictObject(row))`
- 若是 DML，标记 `dataChanged=True`

#### `executemany(sql, values)`
批量执行 SQL（适用于 `INSERT` 等）

---

### 分页与排序

#### `sortSQL(sql, NS)`
添加 `ORDER BY` 子句。

- 支持 `NS['sort']` 为字符串或列表

#### `pagingSQL(sql, paging, NS)`
构建分页 SQL，需配合 `pagingSQLmodel()` 使用。

> 默认为空，需由子类重写提供具体数据库分页语法（如 MySQL 的 `LIMIT OFFSET` 或 Oracle 的 `ROWNUM`）

##### 参数
- `pagename`: 页码参数名（默认 `'page'`）
- `rowsname`: 每页行数（默认 `'rows'`）
- `sortname`: 排序字段（可选）

##### 示例 NS
```python
{
    'page': 2,
    'rows': 20,
    'sort': 'id desc'
}
```

#### `recordCnt(sql)`
包装 SQL 查询总记录数：

```sql
SELECT COUNT(*) rcnt FROM (your_sql) rowcount_table
```

---

### 过滤器支持

#### `filterSQL(sql, filters, NS)`
将一组条件过滤器合并进原 SQL。

每个 filter 是一个带 `${}` 的模板字符串，若其中变量未在 `NS` 中存在，则忽略此条件（替换为 `1=1`）。

最终生成：
```sql
SELECT * FROM (original_sql) filter_table WHERE f1 AND f2 ...
```

---

### Pivot 表格转换

#### `pivotSQL(tablename, rowFields, columnFields, valueFields)`
生成透视表 SQL。

##### 逻辑
1. 查询 `columnFields` 的所有唯一值作为列头
2. 使用 `CASE WHEN` 构造每列聚合
3. 使用 `SUM` 聚合数值字段

##### 示例输出
```sql
SELECT dept, 
       SUM(salary_0) salary_0, -- 北京
       SUM(salary_1) salary_1  -- 上海
FROM (
  SELECT dept,
         CASE WHEN city='北京' THEN salary ELSE 0 END AS salary_0,
         CASE WHEN city='上海' THEN salary ELSE 0 END AS salary_1
  FROM employees
) GROUP BY dept;
```

#### `pivot(desc, ...)` 
执行 pivot 查询并返回结果列表。

---

### 元数据查询方法

#### `tables()`
返回数据库中所有表的列表。

#### `indexes(tablename)`
返回某表的所有索引信息（需子类实现 `indexesSQL()`）

#### `fields(tablename)`
返回字段列表，包含名称、类型（映射后），例如：
```python
[
  {"name": "id", "type": "int"},
  {"name": "name", "type": "string"}
]
```

#### `primary(tablename)`
返回主键字段列表。

#### `fkeys(tablename)`
返回外键关系。

#### `getTableDesc(tablename)`
综合获取表的完整描述（summary + fields + indexes），并缓存至 `metadatas`。

---

### DDL 与表操作

#### `createTable(tabledesc)`
使用模板引擎渲染建表语句并执行。

依赖 `self.ddl_template` 和 `MyTemplateEngine`。

---

### 事务控制

#### `commit()`
提交事务，重置 `dataChanged=False`

#### `rollback()`
回滚事务

---

### CRUD 接口（I/C/R/U/D）

遵循 RESTful 风格简写：

#### `I(tablename)`
等价于 `getTableDesc()` — 获取表结构定义。

#### `C(tablename, ns)`
插入新记录。

- 自动提取 `ns` 中存在于表字段的部分
- 触发 `before` / `after` 钩子函数
- 生成 INSERT 语句

#### `R(tablename, ns, filters=None)`
读取记录。

- 若传入 `page` 参数 → 返回分页 `{total, rows}`
- 否则返回匹配行列表
- 支持简单等值条件或复杂 `DBFilter`

#### `U(tablename, ns)`
更新记录。

- 使用主键做 WHERE 条件
- 更新非主键字段
- 支持钩子函数

#### `D(tablename, ns)`
删除记录。

- 使用主键匹配删除
- 支持钩子函数

---

## 使用示例

### 1. 初始化 SQLor 实例

```python
dbdesc = {
    'dbname': 'mydb'
}
sqlor = SQLor(dbdesc=dbdesc)
sqlor.setCursor(False, conn, cursor)  # 同步模式
```

### 2. 执行参数化查询

```python
sql = "SELECT * FROM users WHERE age > ${min_age}$"
result = []
await sqlor.execute(sql, {'min_age': 18}, lambda rec: result.append(rec))
```

### 3. 分页查询

```python
desc = {
    "sql_string": "SELECT id, name FROM users",
    "sortfield": "name"
}
ns = {"page": 1, "rows": 10}
data = await sqlor.runSQLPaging(desc, ns)
# → {"total": 100, "rows": [...]}
```

### 4. CRUD 操作

```python
# 创建
await sqlor.C("users", {"name": "Alice", "age": 25})

# 读取（分页）
res = await sqlor.R("users", {"page": 1, "rows": 10})

# 更新
await sqlor.U("users", {"id": 1, "age": 26})

# 删除
await sqlor.D("users", {"id": 1})
```

### 5. 获取表结构

```python
desc = await sqlor.I("users")
print(desc['fields'])
```

---

## 异常处理

所有 SQL 执行异常都会被捕获并记录日志：

```python
exception(f"{markedSQL=},{datas=}, {e=}, {fe=}")
raise e
```

推荐外部捕获 `Exception` 或自定义 `SQLException`。

---

## 配置说明

### 占位符配置

| 符号 | 默认 | 用途 |
|------|------|------|
| `sqltp` / `sqlts` | `$[` / `]$` | 控制结构模板（如 IF） |
| `sqlvp` / `sqlvs` | `${` / `}$` | 变量参数占位符 |

示例：
```sql
SELECT * FROM t WHERE status = ${status}$
$[ IF active ]$ AND active = 1 $[ ENDIF ]$
```

### 分页模型扩展

子类应重写 `pagingSQLmodel()` 提供数据库特定的分页语法。

例如 MySQL 子类：
```python
def pagingSQLmodel(self):
    return "SELECT * FROM (%s) t LIMIT ${rows}$ OFFSET ${from_line}$"
```

Oracle 示例：
```python
def pagingSQLmodel(self):
    return """
    SELECT * FROM (
        SELECT a.*, ROWNUM rn FROM (
            %s ORDER BY ${sort}$
        ) a WHERE ROWNUM <= ${end_line}$
    ) WHERE rn > ${from_line}$
    """
```

---

## 总结

`SQLor` 是一个轻量级但功能完整的数据库抽象层，适合用于 Web API 后端、ETL 工具或管理后台的数据访问组件。其优势在于：

✅ 清晰的 CRUD 接口  
✅ 支持异步高性能场景  
✅ 安全的参数绑定  
✅ 易于扩展不同数据库方言  

> ⚠️ **待改进点**：
- 修复 `SQLException.__init__` 错误
- `commit()` 中 `self.datachanged` 应为 `self.dataChanged`
- `pagingdata()` 中 `cnt_desc` 未定义
- `setMeta` 中 `getMeta(self.tablename)` 应为 `self.getMeta(...)`

---

📝 **版本**: v1.0  
📅 **最后更新**: 2025-04-05  
👨‍💻 **作者**: Auto-generated Documentation Tool