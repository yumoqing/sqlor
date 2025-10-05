# SQL 执行工具技术文档

```markdown
# SQL 执行工具（`sql_runner.py`）

一个基于 Python 3 的异步 SQL 脚本执行工具，支持从文件读取 SQL 并在指定数据库中执行，同时允许传入命名参数用于 SQL 模板替换。

---

## 概述

该脚本提供了一个命令行接口，用于执行存储在文件中的 SQL 语句。它通过 `sqlor.dbpools` 模块连接数据库池，并使用 `asyncio` 实现异步执行。支持动态变量注入（通过 `k=v` 参数），适用于自动化部署、数据初始化或批量任务场景。

---

## 依赖说明

- **Python 版本**：Python 3.7+
- **第三方模块**：
  - `sqlor.dbpools.runSQL`：用于异步执行 SQL 的数据库操作模块。
  - 其他内置模块：`sys`, `codecs`, `asyncio`

> ⚠️ 注意：`ProgramPath`, `getConfig`, `DBPools` 等类/函数未在代码中定义，推测属于项目其他模块（如配置管理组件）。需确保这些符号在运行环境中已正确定义并可导入。

---

## 使用方式

### 命令格式

```bash
python3 sql_runner.py <path> <dbname> <sqlfile> [k1=v1] [k2=v2] ...
```

### 参数说明

| 参数 | 描述 |
|------|------|
| `path` | 配置文件路径或程序根路径（用于加载配置） |
| `dbname` | 在配置中定义的数据库连接名称 |
| `sqlfile` | 包含 SQL 语句的文件路径（UTF-8 编码） |
| `k=v` （可选） | 动态键值对参数，可用于 SQL 中的模板替换 |

### 示例调用

```bash
python3 sql_runner.py ./config prod_db ./scripts/init.sql \
    table_name=users \
    batch_size=1000 \
    debug_mode=true
```

上述命令将：
- 加载 `./config` 下的配置；
- 连接名为 `prod_db` 的数据库；
- 读取并执行 `init.sql` 文件中的 SQL；
- 提供三个变量供 SQL 内部引用：`table_name`, `batch_size`, `debug_mode`。

---

## 核心功能

### 1. 初始化 (`appinit`)

```python
def appinit():
    if len(sys.argv) < 4:
        print(f'usage:\n {sys.argv[0]} path dbname sqlfile [k=v ...] \n')
        sys.exit(1)
    p = ProgramPath()
    if len(sys.argv) > 1:
        p = sys.argv[1]
    config = getConfig(p)
    DBPools(config.databases)
```

#### 功能描述：
- 检查命令行参数是否至少包含 `path`, `dbname`, `sqlfile`。
- 获取程序路径 `p`，默认使用 `ProgramPath()` 对象（需外部实现）。
- 使用 `getConfig(p)` 加载配置对象。
- 初始化数据库连接池 `DBPools(config.databases)`。

> ✅ 成功后，全局数据库池准备就绪，可供后续 SQL 执行使用。

---

### 2. 异步 SQL 执行 (`run`)

```python
async def run(ns):
    with codecs.open(sys.argv[3], 'r', 'utf-8') as f:
        sql = f.read()
        await runSQL(sys.argv[2], sql, ns)
```

#### 功能描述：
- 异步函数，使用 `codecs.open` 安全读取 UTF-8 编码的 SQL 文件。
- 将整个文件内容作为字符串加载到内存。
- 调用 `runSQL(dbname, sql, namespace)` 执行 SQL，其中：
  - `dbname`: 来自 `sys.argv[2]`
  - `sql`: 从文件读取的内容
  - `ns`: 用户传入的键值参数字典

> 💡 支持 SQL 模板语法（例如 `${key}` 或 `:key`，具体取决于 `runSQL` 实现），实现动态 SQL 构造。

---

### 3. 主程序入口

```python
if __name__ == '__main__':
    ns = {}
    for x in sys.argv[3:]:
        try:
            k,v = x.split('=')
            ns.update({k:v})
        except Exception as e:
            print(x, 'key-value pair expected')
            print(e)
    
    appinit()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(ns))
```

#### 流程说明：
1. 解析从第 4 个参数开始的所有 `k=v` 形式输入，构建成字典 `ns`。
2. 若解析失败（如缺少 `=`），输出错误信息但不中断程序。
3. 调用 `appinit()` 初始化应用环境和数据库池。
4. 获取事件循环并运行异步任务 `run(ns)`，等待其完成。

---

## SQL 文件要求

- 必须为 **UTF-8 编码**
- 可以包含多条 SQL 语句（由 `runSQL` 决定是否支持批量执行）
- 可包含占位符，如：
  ```sql
  CREATE TABLE IF NOT EXISTS ${table_name};
  INSERT INTO logs VALUES (:msg, :ts);
  ```
  占位符实际语法取决于 `runSQL` 的实现机制（如字符串替换、参数化查询等）

---

## 错误处理与日志

- 参数不足时打印帮助信息并退出（状态码 1）
- `k=v` 解析异常会打印警告，但不会终止程序
- SQL 执行过程中的异常由 `runSQL` 处理，建议其内部进行日志记录或抛出可捕获异常

---

## 注意事项

1. **安全性提醒**：
   - 如果 SQL 使用字符串插值而非参数化查询，存在 SQL 注入风险，请谨慎使用用户输入。
   - 推荐 `runSQL` 使用安全的模板引擎或预编译机制。

2. **性能提示**：
   - 整个 SQL 文件一次性读入内存，不适合超大文件（GB 级别）。
   - 如需流式处理或分批执行，需扩展此脚本。

3. **配置依赖**：
   - `getConfig` 和 `DBPools` 的行为高度依赖项目结构，请确认配置格式正确（如 JSON/YAML 配置文件结构）。

---

## 扩展建议

- 添加 `-h/--help` 参数支持
- 增加日志模块替代 `print`
- 支持 `.sql` 文件通配符批量执行
- 增加 dry-run 模式预览 SQL
- 返回执行结果统计（影响行数、耗时等）

---
```

> 📝 文档版本：v1.0  
> 最后更新：2025-04-05