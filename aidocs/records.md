# `Records` 类技术文档

## 概述

`Records` 是一个用于管理记录集合的 Python 类，它允许将字典形式的数据转换为指定类的实例，并提供统一的存储与迭代访问机制。默认情况下，记录会被转换为 `DictObject` 实例（来自 `appPublic.dictObject` 模块），但支持自定义类作为数据容器。

该类适用于需要动态处理结构化数据的场景，如数据库查询结果、API 响应数据等。

---

## 依赖

```python
from appPublic.dictObject import DictObject
```

- `DictObject`：一个字典式对象封装类，允许通过属性访问字典键值（例如 `obj.name` 而非 `obj['name']`）。
- 若未安装或找不到此模块，请确保 `appPublic` 包已正确安装并可导入。

---

## 类定义

```python
class Records:
    def __init__(self, klass=DictObject):
        self._records = []
        self.klass = klass
```

### 构造函数 `__init__`

#### 参数：
| 参数名 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `klass` | `type` | `DictObject` | 用于将每条记录转换成的对象类型。必须是一个可调用类型（通常是类），接受关键字参数初始化。 |

#### 功能：
- 初始化一个空的记录列表 `_records`。
- 设置用于实例化每条记录的目标类 `klass`。

> 示例：若 `klass=MyDataClass`，则每条记录会以 `MyDataClass(**record_dict)` 的方式创建。

---

## 方法说明

### `add(rec)`

向记录集合中添加一条新记录。

#### 参数：
| 参数名 | 类型 | 说明 |
|--------|------|------|
| `rec` | `dict` | 一个字典类型的记录数据，键对应目标类的初始化参数。 |

#### 行为：
1. 使用 `self.klass(**rec)` 将字典 `rec` 实例化为指定类的对象。
2. 将生成的对象追加到内部列表 `self._records` 中。

#### 异常：
- 若 `rec` 不是字典或其键不匹配 `klass` 所需参数，可能抛出 `TypeError` 或其他初始化异常。

#### 示例：
```python
records = Records()
records.add({"name": "Alice", "age": 30})
# 等效于: obj = DictObject(name="Alice", age=30)
```

---

### `get()`

获取当前所有记录的列表。

#### 返回值：
- `list`：包含所有已添加记录对象的列表，顺序与添加一致。

#### 示例：
```python
all_records = records.get()
for r in all_records:
    print(r.name)
```

---

### `__iter__()`

使 `Records` 实例成为可迭代对象。

#### 返回值：
- 返回自身实例（实现了 `__next__` 的迭代器协议）。

#### 内部行为：
- 初始化迭代器状态变量：
  - `self.start = 0`
  - `self.end = len(self._records)`

> 注意：此实现不是线程安全的，且不支持嵌套循环中的并发迭代（因为状态保存在实例上）。

---

### `__next__()`

实现迭代器协议的下一步方法。

#### 返回值：
- 当还有未遍历记录时，返回下一个记录对象。
- 遍历完成后，抛出 `StopIteration` 异常以终止迭代。

#### 逻辑流程：
```text
if self.start < self.end:
    返回 self._records[self.start] 并递增索引
else:
    抛出 StopIteration
```

#### 示例用法：
```python
for record in records:
    print(record)
```

---

## 使用示例

### 示例 1：使用默认 `DictObject`

```python
from appPublic.dictObject import DictObject
from your_module import Records

records = Records()
records.add({"id": 1, "name": "Product A"})
records.add({"id": 2, "name": "Product B"})

for r in records:
    print(r.id, r.name)
# 输出:
# 1 Product A
# 2 Product B
```

### 示例 2：使用自定义类

```python
class User:
    def __init__(self, name, email):
        self.name = name
        self.email = email

    def __repr__(self):
        return f"<User {self.name} ({self.email})>"

users = Records(klass=User)
users.add({"name": "Bob", "email": "bob@example.com"})
users.add({"name": "Charlie", "email": "charlie@example.com"})

print(users.get())
# 输出: [<User Bob (bob@example.com)>, <User Charlie (charlie@example.com)>]
```

---

## 注意事项

1. **非线程安全**  
   `__iter__` 和 `__next__` 将迭代状态（`start`, `end`）存储在实例上，因此多个同时进行的迭代会导致状态冲突。

2. **不可重入迭代**  
   在一次迭代未完成前再次调用 `iter()` 会覆盖原有状态。建议改用 `iter(records.get())` 获取独立迭代器。

3. **性能提示**  
   `get()` 返回的是原始列表引用，修改该列表会影响内部状态。如需保护数据，建议返回副本：`return self._records[:]`

4. **扩展建议**  
   可考虑实现 `__len__`、`__getitem__` 支持更多序列操作，例如：
   ```python
   def __len__(self):
       return len(self._records)

   def __getitem__(self, idx):
       return self._records[idx]
   ```

---

## 版本历史

| 版本 | 修改内容 |
|------|---------|
| 1.0 | 初始版本，支持基本记录管理与迭代功能 |

---

## 许可证

请参考项目整体许可证文件。