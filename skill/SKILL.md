---
name: sqlor-repo
version: 1.0.0
description: SQLOR 仓库：68 个文件，主语言Python 36，Shell 1，JSON 1
trigger_conditions:
  - 需要理解、修改或排障 sqlor 的代码
  - 在 sqlor 里定位入口、数据表、对外契约或构建方式
---

# sqlor 仓库技能

> 由仓库实际内容生成的骨架（README/目录/模型定义/契约/构建脚本逐项取证）。新增判断与坑位请直接在下面对应小节补充。

## 定位

**SQLOR**

SQLOR is a database api for python3, it is base on the python's DBAPI2 password in json data is encrypted by aes. First, Specified a server_path folder, under the server_path folder, need a named by "conf" subfolder. and a config.json file must be in the conf. the config.json need "password_key" and

- 远端：`https://github.com/yumoqing/sqlor.git`
- 默认分支：`main`（共 112 个提交）
- 最近提交：2026-11-15 `eb663c8` fix(sqlor): 查询取消安全 —— 修 description=None 假故障与悬空游标复用
- 规模：68 个文件；Python 36，Shell 1，JSON 1

## 目录结构

- `sqlor/` — Python 包源码（模块本体）（29 个文件）
- `aidocs/` — AI/开发文档（23 个文件）
- `test/` — 测试（5 个文件）
- `tests/` — 测试（3 个文件）
- `docs/` — 文档（2 个文件）
- 根文件：`.gitignore`、`README.md`、`build.sh`、`databases.json`、`pyproject.toml`、`setup.cfg`

## 构建与运行

- `build.sh`（一键部署）
- `pyproject.toml`：`?` v?，依赖 0 项
- Python 包 `sqlor/`：`__init__.py`、`aiopostgresqlor.py`、`aiosqliteor.py`、`clickhouseor.py`、`const.py`、`dbloader.py`、`dbpassword.py`、`dbpools.old.py`

## README 要点与注意（摘自 `README.md`）

- 章节：Features、requirements、Supported Database Types、Database Description json format、Using、scripts、API、class

## 建议阅读顺序

1. `README.md`
2. `docs/`
3. `aidocs/`
4. `build.sh`

