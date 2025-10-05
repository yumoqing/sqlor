# `CRUD` 模块技术文档

> **文件编码：UTF-8**

该模块提供了一个基于数据库连接池的通用增删改查（CRUD）操作类，支持异步执行、字段类型转换、主外键处理、数据网格生成等功能。适用于 Web 后端服务中对数据库表进行标准化访问。

---

## 目录

- [1. 依赖说明](#1-依赖说明)
- [2. 全局变量](#2-全局变量)
  - [`toStringFuncs`](#tostringfuncs)
  - [`fromStringFuncs`](#fromstringfuncs)
- [3. 异常定义](#3-异常定义)
  - [`DatabaseNotfound`](#databasenotfound)
- [4. 核心类：`CRUD`](#4-核心类crud)
  - [构造函数 `__init__`](#构造函数-__init__)
  - [方法列表](#方法列表)
    - [`primaryKey()`](#async-primarykey)
    - [`forignKeys()`](#async-forignkeys)
    - [`I()`](#async-i)
    - [`fromStr(data)`](#async-fromstrdata)
    - [`toStr(data)`](#async-tostrdata)
    - [`datagrid(request, targeti, **kw)`](#async-datagridrequest-targeti--kw)
    - [`defaultIOField(f)`](#defaultiofieldf)
    - [`C(rec, **kw)`](#async-crec--kw)
    - [`R(filters=None, NS={}, **kw)`](#async-rfiltersnone-ns---kw)
    - [`U(data, **kw)`](#async-udata--kw)
    - [`D(data, **kw)`](#async-ddata--kw)

- [5. 示例用法](#5-示例用法)
- [6. 注意事项与扩展机制](#6-注意事项与扩展机制)

---

## 1. 依赖说明

本模块依赖以下内部和外部组件：

| 模块 | 用途 |
|------|------|
| `.dbpools.DBPools` | 数据库连接池管理器 |
| `.const.ROWS` | 默认每页行数常量 |
| `.filter.DBFilter` | SQL 查询条件过滤器生成工具 |
| `appPublic.objectAction.ObjectAction` | 对象行为钩子系统（用于事件拦截与扩展） |
| `appPublic.dictObject.DictObject` | 字典对象封装（未直接使用，可能为遗留导入） |
| `appPublic.timeUtils.date2str, time2str, str2Date` | 时间格式化/解析函数 |
| `appPublic.uniqueID.getID` | 唯一 ID 生成函数 |

---

## 2. 全局变量

### `toStringFuncs`

将不同字段类型的值转为字符串表示的方法映射。

```python
toStringFuncs = {
    'char': None,
    'str': None,
    'short': str,
    'long': str,
    'float': str,
    'date': date2str,
    'time': time2str,
}
```

- 若值为 `None`，表示不需特殊处理。
- 例如：日期类型通过 `date2str` 转换为 `'YYYY-MM-DD'` 格式字符串。

### `fromStringFuncs`

将字符串反向解析为对应类型的数据转换函数。

```python
fromStringFuncs = {
    'short': int,
    'long': int,
    'float': float,
    'date': str2Date,
    'time': str2Date
}
```

- 空字符串会被转换为 `None`。
- 支持自动类型转换，在插入或更新前预处理输入数据。

---

## 3. 异常定义

### `DatabaseNotfound`

当指定的数据库名称在连接池中不存在时抛出此异常。

#### 属性：
- `dbname`: 请求但未找到的数据库名

#### 方法：
- `__str__()`: 返回 `"xxx not found"` 的可读信息。

```python
raise DatabaseNotfound("mydb")
```

---

## 4. 核心类：`CRUD`

```python
class CRUD(object):
    def __init__(self, dbname, tablename, rows=ROWS):
        ...
```

封装了对单个数据库表的标准 CRUD 操作。

### 构造函数 `__init__`

**参数：**
- `dbname` (str): 数据库标识名（必须存在于 DBPools 中）
- `tablename` (str): 表名
- `rows` (int): 分页查询默认条数，默认为 `ROWS` 常量

**逻辑流程：**
1. 初始化连接池实例 `DBPools()`
2. 验证数据库是否存在，若不存在则抛出 `DatabaseNotfound`
3. 缓存表主键信息（延迟加载）

**抛出异常：**
- `DatabaseNotfound`：数据库未注册

---

### 方法列表

#### `async primaryKey(**kw)`

获取当前表的主键字段信息。

- 第一次调用时从数据库元数据缓存主键信息。
- 后续调用返回缓存结果。

**返回：**
- 列表，每个元素是包含字段信息的对象（如 `{field_name: "id", ...}`）

> 内部使用 `pool.getTablePrimaryKey(dbname, tablename)` 实现。

---

#### `async forignKeys(**kw)`

获取当前表的所有外键关系。

**返回：**
- 外键描述数据结构（由 `pool.getTableForignKeys()` 提供）

---

#### `async I(**kw)`

获取当前表的字段元信息，并附加主键标记。

##### 功能：
1. 获取所有字段信息（来自 `getTableFields`）
2. 将主键字段添加 `"primarykey": True` 标记
3. 执行对象动作钩子：`{dbname}_{tablename}.tableInfo`

##### 返回：
- 字段信息列表，每个字段包含：
  - `name`: 字段名
  - `title`: 显示标题（若有）
  - `type`: 类型（如 str, float, date 等）
  - `primarykey`: 是否为主键（bool）

##### 示例输出片段：
```json
[
  {
    "name": "id",
    "title": "ID",
    "type": "short",
    "primarykey": true
  },
  ...
]
```

---

#### `async fromStr(data)`

将字符串形式的数据字典转换为强类型数据。

**参数：**
- `data` (dict): 键值对，通常来自 HTTP 请求参数

**处理规则：**
- 空字符串 → `None`
- 根据字段类型应用 `fromStringFuncs` 转换（如 `int`, `float`, `str2Date`）

**返回：**
- 新字典，已做类型转换

> 常用于 C/U 接口前的数据预处理。

---

#### `async toStr(data)`

将原始数据转换为适合前端展示的字符串格式。

**参数：**
- `data` (dict): 数据记录

**处理规则：**
- 使用 `toStringFuncs` 将非字符串字段转为字符串（如日期→'2025-04-05'）

**返回：**
- 可序列化的字符串化字典

> 常用于 R 接口后数据输出前的格式化。

---

#### `async datagrid(request, targeti, **kw)`

生成一个前端可用的“数据表格”配置对象，兼容 EasyUI 或类似框架。

##### 参数：
- `request`: 当前请求对象（用于生成绝对 URL）
- `targeti`: 目标 DOM 容器 ID
- `**kw`: 其他传递参数

##### 返回：
一个嵌套结构，描述 DataGrid 组件的模板配置，包括：

| 属性 | 说明 |
|------|------|
| `tmplname` | 使用的模板名 |
| `data.__ctmpl__` | 组件类型标识 |
| `data.__target__` | 渲染目标 |
| `data.data.url` | 查询接口地址 (`./RP.dspy`) |
| `data.data.deleteUrl` | 删除接口 |
| `data.data.addUrl` | 添加接口 |
| `data.data.updateUrl` | 更新接口 |
| `data.data.idField` | 主键字段名 |
| `data.data.fields` | 字段列定义（通过 `defaultIOField` 生成） |
| `data.data.toolbar` | 工具栏按钮（增删上下移动） |
| `data.data.options.pageSize` | 分页大小 |

> 最终通过 `oa.execute(id, 'datagrid', data)` 允许外部插件定制布局。

---

#### `defaultIOField(f)`

根据字段元信息生成默认的前端显示字段配置。

**参数：**
- `f` (dict): 单个字段元信息

**返回：**
标准字段配置对象，含以下字段：

| 字段 | 说明 |
|------|------|
| `name` | 字段名 |
| `label` | 显示标签（取自 `title`） |
| `primarykey` | 是否为主键 |
| `hidden` | 是否隐藏（默认否） |
| `sortable` | 是否可排序 |
| `align` | 文本对齐方式（数值右对齐） |
| `iotype` | 输入类型（目前统一为 `"text"`） |

> 不同类型字段有细微差异：
- `str`: 居中对齐
- 数值型（`short`, `long`, `float`）: 右对齐
- 其他: 居中对齐

---

#### `async C(rec, **kw)`

创建一条新记录（Create）。

##### 参数：
- `rec` (dict): 待插入数据（字段名不区分大小写）
- `**kw`: 额外参数（传给底层 SQL 执行）

##### 流程：
1. 转换键名为小写
2. 若主键为空，则自动生成唯一 ID（`getID()`）
3. 触发 `beforeAdd` 钩子
4. 构造并执行插入语句：
   ```sql
   INSERT INTO table (col1, col2) VALUES (${col1}$, ${col2}$)
   ```
5. 触发 `afterAdd` 钩子
6. 返回主键值 `{pk_field: value}`

##### 返回：
- 成功时返回主键字典；失败抛异常。

---

#### `async R(filters=None, NS={}, **kw)`

检索数据（Retrieve），支持条件查询和分页。

##### 参数：
- `filters`: 自定义过滤表达式（高级语法，见 `DBFilter`）
- `NS`: 查询参数字典（如 `{"name": "Alice"}`）
- `**kw`: 其他选项

##### 特殊处理：
- `NS['__id']` → 自动映射到主键字段
- `NS['page']` 存在 → 启用分页模式
- 无排序字段时，默认按主键排序

##### 查询逻辑：
- 若无 `filters`，则自动生成等值匹配条件（`field = $field$`）
- 使用 `DBFilter` 解析复杂条件（如范围、模糊匹配）

##### 执行分支：
- 分页请求 → 调用 `runSQLPaging`
- 普通查询 → 调用 `runSQL`

##### 生命周期钩子：
- `beforeRetrieve`
- `afterRetrieve`

##### 返回：
- 结果集（可能是分页对象，含 `total`, `rows`）

---

#### `async U(data, **kw)`

更新记录（Update）。

##### 参数：
- `data` (dict): 包含主键和其他待更新字段

##### 流程：
1. 分离主键字段和更新字段
2. 构造 WHERE 条件（主键相等）
3. 构造 SET 子句（仅非主键字段）
4. 执行更新语句：
   ```sql
   UPDATE table SET col1=${col1}$ WHERE id=${id}$
   ```
5. 触发 `beforeUpdate` / `afterUpdate` 钩子

##### 返回：
- 更新后的完整数据对象

---

#### `async D(data, **kw)`

删除记录（Delete）。

##### 参数：
- `data` (dict): 包含主键字段的字典

##### 流程：
1. 构造 WHERE 条件（所有主键字段 AND 连接）
2. 执行删除语句：
   ```sql
   DELETE FROM table WHERE id=${id}$
   ```
3. 触发 `beforeDelete` / `afterDelete` 钩子

##### 返回：
- 被删除的数据副本（可用于日志或通知）

> ⚠️ 注意：未实现软删除，为物理删除。

---

## 5. 示例用法

```python
# 初始化数据库连接池（仅一次）
DBPools({
    "ambi": {
        "driver": "pymssql",
        "coding": "utf-8",
        "dbname": "ambi",
        "kwargs": {
            "user": "ymq",
            "password": "ymq123",
            "host": "localhost",
            "database": "ambi"
        }
    }
})

# 创建 CRUD 实例
crud = CRUD('ambi', 'cashflow')

# 查询全部数据（分页）
data = await crud.R(NS={'page': 1})
print(f"总数: {data.total}")
for row in data.rows:
    print(row.balance, row.asid)

# 插入一条记录
new_id = await crud.C({
    "balance": 1000.5,
    "asid": "A001"
})
print("新建ID:", new_id)

# 更新
await crud.U({
    "id": 123,
    "balance": 1500.0
})

# 删除
await crud.D({"id": 123})
```

---

## 6. 注意事项与扩展机制

### ✅ 已支持特性：
- ✅ 异步非阻塞 I/O
- ✅ 多数据库连接池管理
- ✅ 自动主键识别与处理
- ✅ 输入/输出类型自动转换
- ✅ 支持分页、排序、条件查询
- ✅ 前端 DataGrid 快速生成
- ✅ 通过 `ObjectAction` 实现事件钩子扩展：
  - `beforeAdd`, `afterAdd`
  - `beforeRetrieve`, `afterRetrieve`
  - `beforeUpdate`, `afterUpdate`
  - `beforeDelete`, `afterDelete`
  - `tableInfo`, `datagrid`

### ⚠️ 注意事项：
1. **主键要求**：所有表应明确定义主键，否则部分功能异常。
2. **字段大小写敏感性**：输入建议统一小写，内部会做 `.lower()` 处理。
3. **安全机制**：使用 `${field}$` 占位符 + 参数绑定防止 SQL 注入（依赖底层 `inSqlor` 实现）。
4. **时间格式**：`date` 和 `time` 类型需确保 `str2Date` 和 `date2str` 支持正确格式。
5. **外键未用于级联操作**：当前仅提供信息获取，不自动处理关联数据。

### 🔧 扩展建议：
- 在 `ObjectAction` 中注册钩子函数以增强业务逻辑：
  ```python
  oa.bind("ambi_cashflow.beforeAdd", my_audit_log)
  ```
- 自定义 `defaultIOField` 输出更多控件类型（如 dropdown, datebox）。
- 扩展 `toStringFuncs` / `fromStringFuncs` 支持更多类型（如 bool, json）。

---

## 附录：钩子事件命名规范

| 事件点 | 触发时机 | 推荐用途 |
|-------|---------|--------|
| `{db}_{table}.tableInfo` | 获取字段信息后 | 修改字段显示属性 |
| `{db}_{table}.datagrid` | 生成 datagrid 配置后 | 自定义 UI 布局 |
| `{db}_{table}.beforeAdd` | 插入前 | 数据校验、权限检查、默认值填充 |
| `{db}_{table}.afterAdd` | 插入成功后 | 日志记录、消息通知 |
| `{db}_{table}.beforeRetrieve` | 查询前 | 动态过滤（如租户隔离） |
| `{db}_{table}.afterRetrieve` | 查询返回前 | 敏感字段脱敏 |
| `{db}_{table}.beforeUpdate` | 更新前 | 审计、状态流转控制 |
| `{db}_{table}.afterUpdate` | 更新完成后 | 发布事件 |
| `{db}_{table}.beforeDelete` | 删除前 | 软删除替换、引用检测 |
| `{db}_{table}.afterDelete` | 删除后 | 清理关联资源 |

---

> 📝 文档版本：v1.0  
> © 2025 应用公共平台开发组