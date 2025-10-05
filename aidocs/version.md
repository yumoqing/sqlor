# 技术文档：`fixed_sor.py`

## 概述

本模块 `fixed_sor.py` 是一个用于解决数值计算中迭代方法（如逐次超松弛法，Successive Over-Relaxation, SOR）相关问题的 Python 脚本。该版本主要修复了之前版本中与 C 函数调用相关的 Bug，提升了代码的稳定性与兼容性。

---

## 版本信息

- **当前版本**：`0.1.3`
- **更新内容**：
  - 修复了与底层 C 函数交互时出现的 Bug。
  - 增强了异常处理机制，避免因类型不匹配或内存访问错误导致程序崩溃。
  - 提高了与外部 C 扩展模块的兼容性。

> ⚠️ 注意：此修复主要影响使用 C 扩展进行高性能计算的用户。纯 Python 用户通常不受影响，但仍建议升级以获得更好的稳定性。

---

## 安装与依赖

### 依赖项

- Python >= 3.7
- （可选）Cython 或 ctypes 支持（若使用 C 扩展）

### 安装方式

```bash
# 通过 pip 安装（假设已发布到 PyPI）
pip install fixed-sor==0.1.3

# 或本地安装
python setup.py install
```

---

## 使用示例

```python
import fixed_sor

# 示例：求解线性方程组 Ax = b 使用 SOR 方法
A = [[4, -1, 0],
     [-1, 4, -1],
     [0, -1, 4]]
b = [15, 10, 15]

x = fixed_sor.solve(A, b, omega=1.2, max_iter=1000, tol=1e-6)
print("解为:", x)
```

---

## API 参考

### `solve(A, b, omega=1.0, max_iter=1000, tol=1e-6)`

使用 SOR 方法求解线性方程组 $Ax = b$。

#### 参数：

| 参数        | 类型       | 说明 |
|-------------|------------|------|
| `A`         | list/list of lists 或 numpy.ndarray | 系数矩阵（必须为方阵且对角占优或正定） |
| `b`         | list 或 numpy.ndarray | 常数向量 |
| `omega`     | float      | 松弛因子，默认为 1.0（即 Gauss-Seidel 方法） |
| `max_iter`  | int        | 最大迭代次数，默认 1000 |
| `tol`       | float      | 收敛容差，默认 1e-6 |

#### 返回值：

- `list`：解向量 $x$

#### 异常：

- `ValueError`：输入矩阵维度不匹配或不满足 SOR 方法前提条件。
- `RuntimeError`：迭代未在最大次数内收敛。

---

## 更新日志（Changelog）

### v0.1.3 — 2025-04-05
- ✅ 修复：C 函数接口中的内存访问越界问题
- ✅ 修复：数据类型转换错误（Python 到 C 的 double 数组传递）
- 🛠️ 优化：提升与 NumPy 数组的兼容性
- 📚 更新文档

### v0.1.2 — 2024-XX-XX
- 添加基本 SOR 实现
- 支持列表和 NumPy 输入

---

## 开发与贡献

欢迎提交 Issue 和 Pull Request。请确保测试覆盖所有修改路径，特别是涉及 C 接口的部分。

GitHub 仓库：`https://github.com/username/fixed-sor`

---

## 许可证

MIT License

---

> **备注**：本模块名称“fixed”强调其主要目标是修复先前版本中的关键缺陷，后续版本将逐步增强功能与性能。