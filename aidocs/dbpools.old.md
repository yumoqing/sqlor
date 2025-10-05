以下是为提供的 Python 异步数据库连接与操作代码编写的 **Markdown 格式技术文档**，涵盖模块功能、类结构、核心方法说明及使用示例。

---

# 📚 `dbpool` 模块技术文档

> 基于异步 I/O 的通用数据库连接池与 SQL 执行封装系统  
> 支持多种数据库驱动（SQLite、MySQL、PostgreSQL、Oracle、SQL Server）的同步/异步模式

---

## 🔧 概述

本模块提供了一个可扩展、高性能的异步数据库访问框架，主要特性包括：

- ✅ 多数据库支持：通过插件式设计支持多种数据库
- ✅ 异步连接池管理（基于 `asyncio.Queue`）
- ✅ 自动重连与健康检测机制
- ✅ 单例全局连接池管理器 `DBPools`
- ✅ 装饰器简化 SQL 执行流程
- ✅ 元数据缓存优化表结构查询性能
- ✅ 事务自动提交/回滚控制

适用于高并发 Web 应用或微服务中对数据库资源进行统一管理和高效复用。

---

## 📦 导入依赖

```python
import asyncio
from functools import wraps
from contextlib import asynccontextmanager

# 内部工具库
from appPublic.myImport import myImport
from appPublic.dictObject import DictObject
from appPublic.Singleton import SingletonDecorator
from appPublic.myjson import loadf
from appPublic.jsonConfig import getConfig
from appPublic.rc4 import unpassword
from appPublic.log import exception
```

外部依赖：
- `aiosqlite`, `aiomysql`, `asyncpg` 等异步驱动需根据实际数据库安装

---

## 🏗️ 核心组件架构

| 组件 | 功能 |
|------|------|
| `SQLor` 子类 | 数据库抽象层，封装增删改查逻辑 |
| `sqlorFactory()` | 工厂函数，根据配置创建对应的 `SQLor` 实例 |
| `LifeConnect` | 封装单个连接的生命期与可用性测试 |
| `ConnectionPool` | 连接池实现，维护固定数量的活跃连接 |
| `DBPools` | 单例全局连接池管理器，支持多数据库 |
| 装饰器 (`@runSQL`, `@inSqlor`) | 简化业务函数中的数据库调用 |

---

## 🧱 抽象接口：`SQLor` 及其子类

所有数据库适配器继承自基类 `SQLor`，并实现以下关键方法：

```python
class SQLor:
    def isMe(driver_name: str) -> bool: ...
    async def tables() -> List[str]: ...
    async def fields(table: str) -> List[FieldInfo]: ...
    async def primary(table: str) -> List[str]: ...
    async def indexes(table: str) -> List[IndexInfo]: ...
    async def fkeys(table: str) -> List[ForeignKeyInfo]: ...
    async def runSQL(desc: dict, ns: dict) -> Any: ...
    async def runSQLPaging(desc: dict, ns: dict) -> Dict: ...
```

### 当前支持的数据库驱动

| 驱动名 | 类 | 文件 |
|--------|----|------|
| sqlite3 | `SQLite3or` / `Aiosqliteor` | `.sqlite3or`, `.aiosqliteor` |
| mysql | `MySqlor` / `AioMysqlor` | `.mysqlor`, `.aiomysqlor` |
| postgresql | `AioPostgresqlor` | `.aiopostgresqlor` |
| mssql | `MsSqlor` | `.mssqlor` |
| oracle | `Oracleor` | `.oracleor` |

> 同步和异步版本分别处理阻塞/非阻塞场景。

---

## ⚙️ 工厂函数

### `sqlorFactory(dbdesc) → SQLor`

根据数据库描述字典动态选择合适的 `SQLor` 子类实例。

#### 参数

| 参数 | 类型 | 说明 |
|------|------|------|
| `dbdesc` | `dict` | 包含 `'driver'` 和 `'kwargs'` 的数据库连接信息 |

#### 示例

```python
dbdesc = {
    "driver": "aiomysql",
    "kwargs": {
        "host": "localhost",
        "port": 3306,
        "user": "root",
        "password": "enc:xxxxx",
        "database": "test"
    },
    "async_mode": True
}
sor = sqlorFactory(dbdesc)
```

> 若未找到匹配驱动，则返回默认 `SQLor` 实例。

---

### `sqlorFromFile(dbdef_file, coding='utf8') → SQLor`

从 JSON 文件加载数据库配置并初始化 `SQLor` 实例。

#### 参数

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `dbdef_file` | `str` | - | JSON 配置文件路径 |
| `coding` | `str` | `'utf8'` | 文件编码格式 |

#### 示例文件内容 (`config/db.json`)
```json
{
  "driver": "sqlite3",
  "kwargs": {
    "dbname": "/tmp/test.db"
  }
}
```

#### 使用方式
```python
sor = await sqlorFromFile("config/db.json")
```

---

## 🔌 连接生命期管理：`LifeConnect`

封装单个数据库连接的生命周期，具备自动重建能力。

### 初始化参数

| 属性 | 说明 |
|------|------|
| `connfunc` | 创建连接的函数（如 `aiomysql.connect`） |
| `kw` | 传递给 `connfunc` 的关键字参数 |
| `use_max` | 最大使用次数后强制关闭 |
| `async_mode` | 是否启用异步模式 |

### 方法

| 方法 | 返回类型 | 说明 |
|------|----------|------|
| `use()` | `awaitable<connection>` | 获取一个有效连接，失败时尝试重建 |
| `free(conn)` | `None` | 标记连接已释放（暂不关闭） |
| `testok()` | `awaitable<bool>` | 测试连接是否正常（执行 `SELECT 1`） |
| `_mkconn()` | `awaitable` | 内部创建新连接 |

> 在获取连接时会自动探测断连并尝试重新建立最多 4 次。

---

## 🔄 连接池：`ConnectionPool`

每个数据库拥有独立的连接池，基于 `asyncio.Queue` 实现先进先出调度。

### 初始化参数

| 参数 | 说明 |
|------|------|
| `dbdesc` | 数据库配置对象 |
| `loop` | 事件循环引用 |

### 属性

| 属性 | 说明 |
|------|------|
| `maxconn` | 最大连接数（默认 5） |
| `maxuse` | 单连接最大使用次数（默认 1000） |
| `_pool` | 存放 `LifeConnect` 实例的队列 |

### 关键方法

| 方法 | 说明 |
|------|------|
| `connect()` | 创建新的 `LifeConnect` 并放入池中 |
| `aquire()` | 异步获取可用连接 |
| `release(conn)` | 释放连接回池中 |
| `isEmpty()` / `isFull()` | 查询池状态 |

> 注意：当前未启用锁保护共享状态，建议在单线程协程环境下运行。

---

## 🌐 全局连接池管理器：`DBPools`（单例）

使用 `@SingletonDecorator` 保证全局唯一实例。

### 构造参数

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `databases` | `dict` | `{}` | 名称映射到数据库配置 |
| `max_connect` | `int` | `100` | 全局限流信号量 |
| `loop` | `asyncio.AbstractEventLoop` | `get_event_loop()` | 事件循环 |

### 示例配置

```python
databases = {
    'default': {
        'driver': 'aiomysql',
        'async_mode': True,
        'kwargs': { ... }
    },
    'local_sqlite': {
        'driver': 'sqlite3',
        'async_mode': False,
        'kwargs': {'dbname': '/tmp/app.db'}
    }
}
pools = DBPools(databases=databases)
```

---

## 💡 核心 API 方法

### `getSqlor(name) → SQLor`

获取指定名称的数据库操作对象（带连接绑定）。

```python
sor = await pools.getSqlor('default')
```

> 自动申请连接、游标，并设置上下文。

---

### `freeSqlor(sor)`

释放 `SQLor` 占用的连接资源。

```python
await pools.freeSqlor(sor)
```

---

### `@sqlorContext(name) → Async Context Manager`

推荐使用的上下文管理器，自动处理连接获取、事务提交/回滚、异常捕获与释放。

#### 示例

```python
async with pools.sqlorContext('default') as sor:
    ret = await sor.runSQL({"sql_string": "SELECT * FROM users"})
```

> - 出现异常且有数据变更 → 自动 `rollback`
> - 正常退出 → 自动 `commit`
> - 总是释放连接

---

### `useOrGetSor(dbname, **kw) → (sor, commit_flag)`

内部工具函数：若传入了 `sor` 则复用，否则新建。

用于嵌套调用时避免重复连接开销。

---

## 🎯 装饰器 API（简化开发）

### `@inSqlor(func)`

装饰业务函数，自动注入 `sor` 对象，支持事务控制。

```python
@pools.inSqlor
async def get_user(dbname, NS, user_id, **kw):
    sor = kw['sor']
    return await sor.selectOne("users", {"id": user_id})
```

调用方式：
```python
ret = await get_user('default', {}, user_id=123)
```

> 若未传 `sor`，则自动申请；否则复用传入的 `sor`

---

### `@runSQL(func)`

装饰函数返回 SQL 描述对象，然后执行它。

```python
@pools.runSQL
async def list_users(dbname, NS, dept_id, **kw):
    return {
        "sql_string": "SELECT * FROM users WHERE dept_id=?",
        "args": [dept_id]
    }

# 调用
result = await list_users('default', {}, dept_id=5)
```

> 自动执行 SQL、提交事务（如需）、释放连接。

---

### `@runSQLPaging(func)`

分页查询专用装饰器，结合 `sor.runSQLPaging()` 使用。

```python
@pools.runSQLPaging
async def search_users_paged(dbname, NS, keyword, page=1, pagesize=10):
    return {
        "sql_string": "SELECT id,name FROM users WHERE name LIKE ?",
        "args": [f"%{keyword}%"]
    }
```

返回格式：
```json
{
  "page": 1,
  "pagesize": 10,
  "total": 87,
  "data": [...]
}
```

---

### `@runSQLResultFields(func)`

获取查询结果字段定义（元数据），常用于动态表单生成。

```python
@pools.runSQLResultFields
async def describe_query(dbname, NS, table):
    return {"sql_string": f"SELECT * FROM {table} LIMIT 1"}
```

---

## 🔍 元数据查询接口

提升性能：结果缓存在内存中。

| 方法 | 作用 |
|------|------|
| `getTables(dbname)` | 获取所有表名列表 |
| `getTableFields(dbname, tblname)` | 获取某表字段结构 |
| `getTablePrimaryKey(dbname, tblname)` | 获取主键字段 |
| `getTableIndexes(dbname, tblname)` | 获取索引信息 |
| `getTableForignKeys(dbname, tblname)` | 获取外键关系 |

> 所有方法均带本地缓存，默认以 `"dbname:tablename"` 为键。

---

## 🛠️ 辅助函数

### `runSQL(dbname, sql, ns={}, sor=None)`

快速执行一条原始 SQL。

```python
result = await runSQL(
    dbname='default',
    sql='SELECT COUNT(*) AS cnt FROM users'
)
```

> 推荐仅用于简单脚本或调试。

---

### `runSQLPaging(dbname, sql, ns={}, sor=None)`

快速执行分页 SQL。

```python
paged = await runSQLPaging(
    dbname='default',
    sql='SELECT id,name FROM users ORDER BY id',
    ns={'page': 2, 'pagesize': 10}
)
```

---

## 🔐 安全相关

- 密码字段支持加密标记：`"password": "enc:xxxxxx"`
- 使用 `unpassword(pw)` 解密（RC4 加密算法，需确保密钥一致）
- 不建议生产环境明文存储密码

---

## 📐 配置结构规范（JSON）

```json
{
  "driver": "aiomysql",
  "async_mode": true,
  "maxconn": 10,
  "maxuse": 500,
  "kwargs": {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "appuser",
    "password": "enc:xxxxxxxx",
    "database": "myapp_db"
  }
}
```

> 支持字段：
- `driver`: 必须匹配导入路径下的模块名
- `async_mode`: 控制异步行为
- `kwargs`: 透传给底层驱动

---

## 🧪 使用示例

### 1. 初始化连接池

```python
from your_module import DBPools

config = loadf("db.json")  # 加载配置
pools = DBPools(databases={"prod": config})
```

### 2. 执行普通查询

```python
async def main():
    async with pools.sqlorContext("prod") as sor:
        users = await sor.select("users", where={"status": 1})
        print(users)
```

### 3. 分页查询用户

```python
@pools.runSQLPaging
async def query_active_users(dbname, NS, status=1):
    return {
        "sql_string": "SELECT id,name,email FROM users WHERE status=?",
        "args": [status]
    }

result = await query_active_users("prod", {"page": 1, "pagesize": 20})
print(result)
```

---

## ⚠️ 注意事项

1. **异步模式必须使用异步驱动**
   - 如 `aiomysql`, `asyncpg`, `aiosqlite`
2. **不要手动调用 `close()` 游标或连接**
   - 应由 `DBPools` 统一管理
3. **避免长时间持有 `sor` 实例**
   - 建议使用 `sqlorContext` 上下文管理器
4. **元数据缓存不会自动刷新**
   - 表结构变更后需重启服务或手动清理 `meta`

---

## 📈 性能建议

- 设置合理的 `maxconn`（通常 ≤ CPU 核心数 × 2）
- `maxuse` 可防止长连接老化导致的问题
- 生产环境建议开启日志监控连接健康状况

---

## 📎 附录 A：错误处理策略

| 场景 | 处理方式 |
|------|-----------|
| 连接中断 | 尝试重建最多 4 次 |
| 查询异常 | 记录 traceback 日志 |
| 事务失败 | 自动 rollback 并抛出原异常 |
| 密码解密失败 | 抛出异常（需检查 RC4 密钥） |

---

## 📎 附录 B：未来改进方向

- [ ] 添加连接池监控指标（Prometheus）
- [ ] 支持读写分离
- [ ] 更细粒度的超时控制
- [ ] SQL 拦截器（审计/日志）
- [ ] 连接泄漏检测机制

---

> ✅ 文档版本：v1.0  
> 📅 更新时间：2025-04-05  
> © 2025 Your Company. All Rights Reserved.

--- 

📌 提示：将此文档保存为 `README.md` 或集成至 Sphinx/Wiki 中便于团队查阅。