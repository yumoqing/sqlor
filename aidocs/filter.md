# DBFilter 过滤器解释器技术文档

## 概述

`DBFilter` 是一个用于将 JSON 格式的 SQL 查询过滤条件解析为参数化 SQL 片段的 Python 类。它支持常见的逻辑运算符（AND、OR、NOT）和关系表达式（如 `=`, `<>`, `IN`, `LIKE` 等），适用于动态构建安全的数据库查询语句。

该模块通过递归遍历 JSON 结构，生成可嵌入 SQL 语句中的字符串，并提取常量与变量绑定信息，便于后续参数绑定或模板渲染。

---

## 安装依赖

本模块无外部强依赖，但推荐使用 `ujson` 提升性能：

```bash
pip install ujson
```

日志功能依赖于 `appPublic.log.debug`，确保环境中已安装对应库或替换为标准日志模块。

---

## 支持的操作类型

### 1. 逻辑运算符（Logic Operators）

| 操作符 | 说明 |
|--------|------|
| `AND`  | 所有子条件都必须为真 |
| `OR`   | 至少一个子条件为真 |
| `NOT`  | 对单个条件取反 |

### 2. 关系运算符（Relational Operators）

| 运算符        | 含义             |
|---------------|------------------|
| `=`           | 等于             |
| `!=` 或 `<>`  | 不等于           |
| `>`           | 大于             |
| `<`           | 小于             |
| `>=`          | 大于等于         |
| `<=`          | 小于等于         |
| `IN`          | 在某个集合中     |
| `NOT IN`      | 不在某个集合中   |
| `IS NULL`     | 为空             |
| `IS NOT NULL` | 非空             |
| `LIKE`        | 模糊匹配         |
| `NOT LIKE`    | 非模糊匹配       |

> 注意：所有操作符不区分大小写处理，内部统一转为大写进行校验。

---

## 数据结构定义

过滤条件以嵌套 JSON 对象形式表示，基本结构如下：

```json
{
  "AND": [
    { "field": "name", "op": "=", "var": "username" },
    { "field": "status", "op": "=", "const": "active" }
  ]
}
```

### 字段说明

| 字段名 | 类型 | 必需 | 描述 |
|-------|------|------|------|
| `field` | string | 是 | 数据库字段名 |
| `op`    | string | 是 | 操作符（见上表） |
| `const` | any    | 否 | 常量值（直接序列化为占位符） |
| `var`   | string | 否 | 变量名，需在命名空间 `ns` 中提供 |

> - `const` 和 `var` 至少存在其一（除非是 `IS NULL` / `IS NOT NULL` 类型）
> - 使用 `${name}$` 作为参数占位符格式，避免与主流 ORM 冲突

---

## API 文档

### 类：`DBFilter`

#### 初始化方法：`__init__(self, filterjson)`

**参数：**
- `filterjson` (`str` or `dict`)：JSON 字符串或字典格式的过滤条件

**行为：**
- 若输入为字符串，则自动调用 `json.loads()` 解析
- 存储解析后的结构供后续生成 SQL 使用

**示例：**
```python
fj = {"AND": [...]}
dbf = DBFilter(fj)
```

---

#### 方法：`gen(ns={}) -> str or None`

生成参数化的 SQL 过滤片段。

**参数：**
- `ns` (`dict`)：变量命名空间，提供 `var` 到实际值的映射

**返回值：**
- 成功时返回 SQL 表达式字符串（可能包含括号）
- 若无法生成有效 SQL（如变量缺失），返回 `None`
- 所有常量会被分配唯一名称并存入 `self.consts`

**副作用：**
- 更新 `self.consts` 字典，记录所有常量占位符及其值

**示例：**
```python
sql = dbf.gen({'username': 'alice'})
# 输出: "name = ${filter_const_0}$ AND status = 'active'"
```

---

#### 方法：`get_variables() -> dict`

获取过滤器中所有 `var` 映射到 `field` 的变量引用关系。

**返回值：**
- `dict`: `{ variable_name: field_name }`

可用于预检查所需变量是否齐全。

**示例：**
```python
dbf.get_variables()
# 返回: {'username': 'name', 'status': 'status'}
```

---

#### 私有方法：`_genFilterSQL(fj, ns)`

递归生成 SQL 表达式的核心逻辑。

**处理规则：**
- 单键对象优先判断是否为逻辑操作符
- `AND` / `OR`：连接多个子表达式，`OR` 自动加括号
- `NOT`：对子表达式加 `NOT (...)`
- 其他情况交由 `_genFilterItems` 处理

---

#### 私有方法：`_genFilterItems(fj, ns)`

处理单个字段比较表达式。

**逻辑：**
- 校验必填字段：`field`, `op`, (`const` 或 `var`)
- 支持 `IS NULL` / `IS NOT NULL` 无需值
- 若使用 `var`，检查其是否存在于 `ns`
- 若使用 `const`，生成唯一占位符名（如 `filter_const_0`）

---

#### 函数：`default_filterjson(fields: list, ns: dict) -> dict or None`

根据字段白名单和变量空间生成默认等值过滤条件。

**参数：**
- `fields`: 允许参与过滤的字段列表
- `ns`: 当前可用变量字典

**返回值：**
- 匹配字段的等值条件组成的 `AND` 结构
- 若无匹配项返回 `None`
- 单条件时不包装 `AND`

**用途：**
常用于自动生成基于请求参数的简单过滤器。

**示例：**
```python
default_filterjson(['name', 'age'], {'name': 'Bob', 'city': 'Shanghai'})
# 返回:
# {
#   "AND": [
#     {"field": "name", "op": "=", "var": "name"}
#   ]
# }
```

---

## 使用示例

### 示例 1：基础用法

```python
fj = {
  "AND": [
    {
      "field": "field1",
      "op": "=",
      "var": "name"
    },
    {
      "field": "del_flg",
      "op": "=",
      "const": "0"
    }
  ]
}

dbf = DBFilter(fj)
sql = dbf.gen({"name": "joe"})
print(sql)
# 输出: field1 = ${name}$ AND del_flg = ${filter_const_0}$
print(dbf.consts)
# 输出: {'filter_const_0': '0'}
```

### 示例 2：复杂嵌套逻辑

```python
fj = {
  "AND": [
    {
      "OR": [
        {"field": "age", "op": ">=", "const": 18},
        {"field": "role", "op": "IN", "const": ["admin", "mod"]}
      ]
    },
    {
      "NOT": {
        "field": "status",
        "op": "=",
        "var": "blocked"
      }
    }
  ]
}

dbf = DBFilter(fj)
sql = dbf.gen({"blocked": "inactive"})
# 输出:
# ((age >= ${filter_const_0}$ OR role IN ${filter_const_1}$)) AND not (status = ${blocked}$)
```

### 示例 3：NULL 判断

```python
fj = {
  "field": "email",
  "op": "IS NOT NULL"
}

dbf = DBFilter(fj)
sql = dbf.gen({})
# 输出: email IS NOT NULL
```

---

## 错误处理

| 异常场景 | 抛出异常 |
|--------|---------|
| `AND/OR` 的值不是数组或长度小于 2 | `Exception` |
| `NOT` 的值不是对象 | `Exception` |
| 缺少必要字段（`field`, `op`） | `AssertionError` |
| 使用非法操作符 | `AssertionError` |
| `var` 在 `ns` 中不存在 | 返回 `None`（静默跳过） |

---

## 占位符设计

采用 `${name}$` 形式作为参数占位符，优点包括：
- 不与主流 SQL 参数风格冲突（如 `%s`, `?`, `:name`）
- 易于被模板引擎识别和替换
- 支持嵌入任意文本上下文

最终应配合参数绑定系统完成实际值注入。

---

## 设计特点

- **安全性**：所有常量均通过占位符传递，防止 SQL 注入
- **灵活性**：支持任意深度嵌套的布尔逻辑
- **可扩展性**：可通过继承修改 `operators` 或重写生成逻辑
- **轻量级**：无需依赖完整 ORM，适合微服务或中间层使用

---

## 许可证

MIT License（假设项目允许，默认添加）

---

> ✅ 提示：建议结合 Jinja2 或类似模板引擎使用生成的 SQL 片段，实现完整的动态查询构造。