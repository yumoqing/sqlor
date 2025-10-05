以下是为提供的 Python 代码编写的 **Markdown 格式技术文档**，涵盖了模块功能、类说明、函数接口、使用方式及关键设计思想。

---

# `dbor` 模块技术文档

> 数据库操作抽象层与连接池管理（支持同步/异步）

## 概述

`dbor` 是一个轻量级的数据库操作抽象框架，提供统一接口访问多种数据库（如 MySQL、PostgreSQL、SQLite、Oracle、SQL Server），并支持 **同步与异步模式**。该模块通过工厂模式创建数据库适配器，并结合连接池机制提升性能和资源利用率。

主要特性：
- 支持主流关系型数据库
- 同步与异步双模式运行
- 自动连接池管理
- 单例全局数据库池控制器
- 配置驱动式数据库接入
- 安全密码解密（RC4）
- 上下文管理器自动提交/回滚

---

## 依赖说明

```python
import asyncio
from functools import wraps
from contextlib import asynccontextmanager
import threading
```

自定义模块依赖（需确保路径正确）：
- `appPublic.myImport`：动态导入模块
- `appPublic.dictObject`：字典对象封装
- `appPublic.Singleton`：单例装饰器
- `appPublic.myjson.loadf`：加载 JSON 文件
- `appPublic.jsonConfig.getConfig`：配置读取
- `appPublic.rc4.unpassword`：RC4 密码解密
- `appPublic.log.exception`：异常日志记录

---

## 核心组件

### 1. `sqlorFactory(dbdesc) → SQLor`

根据数据库描述信息实例化对应的数据库操作对象。

#### 参数
| 参数名   | 类型     | 说明 |
|--------|--------|----|
| `dbdesc` | `dict` | 数据库配置字典，必须包含 `'driver'` 字段 |

#### 返回值
- 继承自 `SQLor` 的具体数据库操作实例（如 `MySqlor`, `AioMysqlor` 等）
- 若未匹配到子类，则返回基础 `SQLor` 实例

#### 示例配置结构
```python
{
    "driver": "mysql.connector",
    "async_mode": False,
    "kwargs": {
        "host": "localhost",
        "user": "root",
        "password": "enc:xxxxx",  # RC4 加密后的密码
        "database": "testdb"
    }
}
```

---

### 2. `sqlorFromFile(dbdef_file, coding='utf8') → SQLor`

从 JSON 配置文件加载数据库定义并生成对应操作实例。

#### 参数
| 参数名       | 类型     | 默认值   | 说明 |
|------------|--------|--------|----|
| `dbdef_file` | `str`  | -      | JSON 配置文件路径 |
| `coding`     | `str`  | `'utf8'` | 文件编码格式 |

#### 示例
```python
sor = sqlorFromFile('/path/to/db.json')
```

---

## 连接生命周期管理：`LifeConnect`

封装单个数据库连接的生命周期控制，包括健康检测、重连机制、使用计数等。

### 构造函数
```python
LifeConnect(connfunc, kw, use_max=1000, async_mode=False)
```

| 参数         | 类型       | 说明 |
|------------|----------|----|
| `connfunc`   | `callable` | 创建连接的函数（如 `sqlite3.connect`） |
| `kw`         | `dict`     | 连接参数 |
| `use_max`    | `int`      | 最大使用次数后关闭连接，默认 `1000` |
| `async_mode` | `bool`     | 是否异步模式 |

### 方法

| 方法名       | 异步 | 说明 |
|------------|------|----|
| `use()`     | ✅   | 获取可用连接，失败时尝试重建 |
| `free(conn)`| ✅   | 释放连接（暂未启用最大使用限制逻辑） |
| `testok()`  | ✅   | 测试连接是否正常（执行 `SELECT 1`） |
| `_mkconn()` | ✅   | 内部方法：创建新连接 |

> ⚠️ 注意：当前 `free()` 中关于 `use_cnt >= use_max` 的判断被注释，不会触发自动关闭。

---

## 连接池：`ConnectionPool`

管理某一数据库的所有连接，实现连接复用。

### 构造函数
```python
ConnectionPool(dbdesc, loop)
```

| 参数       | 类型            | 说明 |
|----------|---------------|----|
| `dbdesc` | `dict`        | 数据库配置 |
| `loop`   | `asyncio.EventLoop` | 异步事件循环 |

### 属性
| 属性           | 类型               | 说明 |
|--------------|------------------|----|
| `maxconn`      | `int`             | 最大连接数（默认 5） |
| `maxuse`       | `int`             | 每连接最大使用次数 |
| `_pool`        | `asyncio.Queue`   | 存放 `LifeConnect` 实例的队列 |
| `connectObject`| `dict`            | 当前活跃连接映射表 |

### 方法

| 方法             | 异步 | 说明 |
|----------------|------|----|
| `connect()`     | ✅   | 创建一个 `LifeConnect` 并放入池中 |
| `aquire()`      | ✅   | 从池中获取可用连接 |
| `release(conn)` | ✅   | 释放连接回池 |
| `isEmpty()`     | ❌   | 判断池是否为空 |
| `isFull()`      | ❌   | 判断池是否已满 |
| `_fillPool()`   | ✅   | 初始化填充连接池至最大容量 |

> 📝 注：目前 `_fillPool()` 调用后并未实际等待完成，存在潜在问题。

---

## 全局数据库池管理器：`DBPools`（单例）

使用 `@SingletonDecorator` 装饰，保证全局唯一实例，统一管理多个数据库连接池。

### 构造函数
```python
DBPools(databases={}, max_connect=100, loop=None)
```

| 参数           | 类型     | 说明 |
|--------------|--------|----|
| `databases`    | `dict` | 数据库名称 → 配置字典的映射 |
| `max_connect`  | `int`  | 全局最大并发连接数限制 |
| `loop`         | `EventLoop` | 可选事件循环 |

### 属性
| 属性          | 类型                | 说明 |
|-------------|-------------------|----|
| `sema`        | `asyncio.Semaphore` | 控制总连接数信号量 |
| `_cpools`     | `dict`              | 数据库名 → `ConnectionPool` 映射 |
| `databases`   | `dict`              | 所有注册数据库配置 |
| `meta`        | `dict`              | 预留元数据存储 |
| `e_except`    | `Exception`         | 上下文中捕获的异常 |

---

### 核心方法

#### `addDatabase(name, desc)`
注册新的数据库配置。

```python
dbpools.addDatabase('devdb', {
    'driver': 'aiomysql',
    'async_mode': True,
    'kwargs': { ... }
})
```

#### `get_dbname(name) → str or None`
获取数据库逻辑名对应的实际数据库名（`dbname`），若无则返回 `None`。

#### `isAsyncDriver(dbname) → bool`
判断指定数据库是否运行在异步模式下。

#### `getSqlor(name) → SQLor`
获取指定数据库的操作对象（含连接和游标）。

- 使用信号量控制并发
- 自动建立底层连接与游标
- 返回已绑定连接的 `SQLor` 实例

#### `freeSqlor(sor)`
释放由 `getSqlor()` 获取的 `SQLor` 实例，归还连接并释放信号量。

#### `sqlorContext(name)`
**异步上下文管理器**，推荐使用的安全访问方式。

```python
async with dbpools.sqlorContext('mydb') as sor:
    await sor.execute('SELECT * FROM users')
    result = await sor.fetchall()
    # 自动 commit
```

##### 行为逻辑
- 成功退出：自动提交事务（如果 `dataChanged == True`）
- 抛出异常：记录日志 + 回滚事务（如有更改）
- 始终执行：释放连接资源

---

### 内部方法（不建议直接调用）

| 方法                 | 异步 | 说明 |
|--------------------|------|----|
| `_aquireConn(dbname)` | ✅   | 获取连接、游标三元组 `(async_mode, conn, cur)` |
| `_releaseConn(...)`   | ✅   | 关闭游标与连接，归还至池 |

> 🔐 密码处理：在 `_aquireConn` 中自动调用 `unpassword(pw)` 解密。

---

## 支持的数据库驱动

| 驱动模块             | 同步类           | 异步类             | 支持情况 |
|---------------------|----------------|------------------|-------|
| `sqlite3`           | `SQLite3or`    | `Aiosqliteor`    | ✅     |
| `mysql.connector`   | `MySqlor`      | `AioMysqlor`     | ✅     |
| `psycopg2` / `asyncpg` | `SQLor` 扩展 | `AioPostgresqlor`| ✅     |
| `cx_Oracle`         | `Oracleor`     | （暂无异步）       | ✅     |
| `pyodbc` / `pymssql`| `SQLor` 扩展   | `MsSqlor`        | ✅     |

> 💡 提示：所有驱动需继承 `SQLor` 并实现 `isMe(driver_name)` 类方法用于识别。

---

## 使用示例

### 步骤 1：初始化 DBPools（应用启动时）

```python
from appPublic.jsonConfig import getConfig
from your_module import DBPools

config = getConfig()  # 加载全局配置
dbpools = DBPools(databases=config.databases, loop=asyncio.get_event_loop())
```

### 步骤 2：使用上下文访问数据库

```python
async def fetch_users():
    async with dbpools.sqlorContext('users_db') as sor:
        await sor.execute('SELECT id, name FROM users LIMIT 10')
        rows = await sor.fetchall()
        return rows
```

### 步骤 3：手动获取/释放（高级用法）

```python
async def manual_usage():
    sor = await dbpools.getSqlor('reporting')
    try:
        await sor.execute('UPDATE stats SET processed=1 WHERE flag=0')
        await sor.commit()
    except:
        await sor.rollback()
    finally:
        await dbpools.freeSqlor(sor)
```

---

## 安全与日志

- 所有密码字段通过 `unpassword()` 解密（基于 RC4）
- 异常通过 `appPublic.log.exception()` 记录堆栈
- 上下文管理器自动处理事务完整性

---

## 已知问题与改进建议

| 问题 | 描述 | 建议 |
|-----|------|------|
| `free()` 方法中 `use_max` 逻辑被注释 | 连接无法按使用次数回收 | 恢复条件判断逻辑 |
| `_fillPool()` 未 await 循环 | 初始连接可能未完全建立 | 改为批量 `asyncio.gather()` |
| `lock` 相关代码被注释 | 缺少并发保护 | 若多线程需启用锁机制 |
| `print()` 方法仅调试用途 | 应移除或改为日志输出 | 删除或替换为 logger.debug |

---

## 总结

`dbor` 提供了一个灵活、可扩展的数据库访问架构，适用于中小型项目中的多数据库统一管理场景。其核心优势在于：

✅ 统一接口  
✅ 异步友好  
✅ 自动资源管理  
✅ 安全配置支持  

适合集成进 Web 框架（如 FastAPI、Tornado）或后台服务系统中作为 ORM 替代方案。

---

> 文档版本：v1.0  
> 编写时间：2025年4月5日  
> 适用代码版本：请以实际提交为准