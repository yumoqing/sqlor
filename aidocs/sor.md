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

`SQLor` 提供了一个统一接口来执行 SQL 查询和命令，并通过命名参数 `${var}$` 支持模板化 SQL。其主要特点包括：

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
| 参数化 SQL | 使用 `${var}$` 占位符进行变量注入 |
| 异步支持 | 支持 `async/await` 模式运行 SQL |
| 分页查询 | 自动生成分页 SQL（需子类实现模型） |
| 排序与过滤 | 支持动态排序字段和条件表达式 |
| 元数据提取 | 获取表、字段、主键、索引等结构信息 |
| CRUD 封装 | 提供 `C()`(创建), `R()`(读取), `U()`(更新), `D()`(删除) 方法 |
| 回调机制 | 支持逐行处理查询结果 |
| 类型转换 | 自动将数据库类型转为 Python 原生类型 |

---

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
