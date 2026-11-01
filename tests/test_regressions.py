"""sqlor 回归测试（2026-11-01）：锁死三个历史缺陷。

背景（生产事故）：
  pipeline 长任务（20~40 分钟）持有同一个池连接，跑完写交付件时抛
  `pymysql ProgrammingError: Cursor closed` → 交付件丢失 → 引擎造兜底件 → QC 永退。
  根因三条，全部由本文件覆盖：
    ① MySqlor.close() 写错（`self.cursor.close()` 是方法、`await conn.close()` 是同步函数）
       → close 必抛、连接从未被关闭，只靠 GC 兜底；
    ② `commit()` 里写成 `self.datachanged`（拼写错误）→ 标志永不归零；
    ③ `SqlorPool.context()` check-then-await-then-mark：先选 not used、再 await 探活、
       最后才置 used=True → 两个协程拿到同一条连接/同一个 cursor。

跑法（不需要真库、不需要网络）：
    cd <repo> && python3 -m unittest tests.test_regressions -v
"""
import asyncio
import os
import sys
import types
import unittest

_REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _REPO)

# ── appPublic 依赖桩（hermetic）：有没有装 appPublic 都能跑 ──
import importlib


def _ensure_pkg(name):
    mod = sys.modules.get(name)
    if mod is None:
        try:
            mod = importlib.import_module(name)
        except Exception:
            mod = types.ModuleType(name)
            sys.modules[name] = mod
    if not hasattr(mod, '__path__'):
        mod.__path__ = []          # 当成 package，否则 `from appPublic.x import y` 报 not a package
    return mod


class _DictObject(dict):
    def __init__(self, **kw):
        super().__init__(**kw)
        self.__dict__.update(kw)

    def __getattr__(self, k):
        return self.get(k)

    def __setattr__(self, k, v):
        self[k] = v
        self.__dict__[k] = v


class _EvtDisp:
    def __init__(self, *a, **k):
        pass


_AP_SUBS = {
    'worker': {'get_event_loop': lambda: asyncio.get_event_loop(), 'awaitify': lambda f: f},
    'dictObject': {'DictObject': _DictObject},
    'log': {'exception': lambda *a, **k: None, 'debug': lambda *a, **k: None,
            'info': lambda *a, **k: None, 'warning': lambda *a, **k: None},
    'argsConvert': {'ArgsConvert': object, 'ConditionConvert': object},
    'myImport': {'myImport': lambda name, *a, **k: __import__(name)},
    'Singleton': {'SingletonDecorator': lambda cls: cls},
    'myjson': {'loadf': lambda *a, **k: {}},
    'jsonConfig': {'getConfig': lambda *a, **k: types.SimpleNamespace(databases={})},
    'event_dispatcher': {'EventDispatcher': _EvtDisp},
}
_ensure_pkg('appPublic')
for _sub, _attrs in _AP_SUBS.items():
    try:
        importlib.import_module('appPublic.' + _sub)
    except Exception:
        _m = types.ModuleType('appPublic.' + _sub)
        for _k, _v in _attrs.items():
            setattr(_m, _k, _v)
        sys.modules['appPublic.' + _sub] = _m
        setattr(sys.modules['appPublic'], _sub, _m)

class _Permissive(types.ModuleType):
    """任何属性都返回"可调用也可取属性"的桩，满足 appPublic 里杂七杂八的工具函数。"""

    def __getattr__(self, k):
        if k.startswith('__'):
            raise AttributeError(k)
        v = _Permissive(self.__name__ + '.' + k)
        setattr(self, k, v)
        return v

    def __call__(self, *a, **k):
        return _Permissive(self.__name__ + '()')


class _PermissiveLoader:
    def __init__(self, mod):
        self.mod = mod

    def create_module(self, spec):
        return self.mod

    def exec_module(self, module):
        return None


class _AppPublicFinder:
    """兜底：真装 appPublic 时走真的；没装/子模块缺失时按需造桩，保证测试 hermetic。"""

    def find_spec(self, fullname, path=None, target=None):
        if not fullname.startswith('appPublic.'):
            return None
        import importlib.util
        mod = _Permissive(fullname)
        return importlib.util.spec_from_loader(fullname, _PermissiveLoader(mod))


if not any(isinstance(f, _AppPublicFinder) for f in sys.meta_path):
    sys.meta_path.append(_AppPublicFinder())


# ── 驱动依赖桩：sqlor.dbpools 会 import 全部驱动，测试环境不必装齐 ──
def _stub(name, attrs=()):
    try:
        __import__(name)
        return
    except Exception:
        pass
    mod = types.ModuleType(name)
    for a in attrs:
        setattr(mod, a, type(a, (Exception,), {}))
    sys.modules[name] = mod


for _m, _attrs in (
    ('aiomysql', ('Error', 'OperationalError', 'ProgrammingError', 'InterfaceError')),
    ('aiosqlite', ('Error', 'OperationalError')),
    ('pymssql', ('Error', 'OperationalError')),
    ('psycopg', ('Error', 'OperationalError')),
    ('psycopg2', ('Error', 'OperationalError')),
    ('oracledb', ('Error', 'DatabaseError')),
    ('asyncpg', ('PostgresError',)),
    ('aiopg', ('Error', 'OperationalError')),
    ('pymysql', ('Error', 'OperationalError', 'ProgrammingError', 'InterfaceError')),
):
    _stub(_m, _attrs)


class FakeCursor:
    def __init__(self):
        self.closed = False

    async def close(self):
        self.closed = True


class FakeConn:
    """模拟 aiomysql.Connection：close() 是**同步**的（这正是旧 close() 踩的坑）。"""

    def __init__(self):
        self.closed = False
        self.committed = 0
        self.rolledback = 0

    def close(self):
        self.closed = True

    async def commit(self):
        self.committed += 1

    async def rollback(self):
        self.rolledback += 1


def _bare_mysqlor():
    """不走真实 connect，构造一个只有 cur/conn 的 MySqlor 实例。"""
    from sqlor.mysqlor import MySqlor
    sor = object.__new__(MySqlor)
    sor.cur = FakeCursor()
    sor.conn = FakeConn()
    sor.dataChanged = True
    return sor


class TestCloseRegression(unittest.TestCase):
    """① close() 必须真的关闭游标与连接，且不抛异常。"""

    def test_close_closes_cursor_and_connection(self):
        sor = _bare_mysqlor()
        cur, conn = sor.cur, sor.conn
        asyncio.run(sor.close())
        self.assertTrue(cur.closed, "游标没有被关闭")
        self.assertTrue(conn.closed, "连接没有被关闭（旧实现必抛 TypeError）")

    def test_close_is_idempotent_and_never_raises(self):
        sor = _bare_mysqlor()
        asyncio.run(sor.close())
        asyncio.run(sor.close())          # 第二次不应抛
        self.assertTrue(sor.conn is None or sor.conn.closed)

    def test_close_tolerates_missing_attrs(self):
        from sqlor.mysqlor import MySqlor
        sor = object.__new__(MySqlor)
        asyncio.run(sor.close())          # 无 cur/conn 也不许抛


class TestCommitFlagRegression(unittest.TestCase):
    """② commit() 后 dataChanged 必须归零（旧实现写成 datachanged，永不归零）。"""

    def test_commit_resets_dataChanged(self):
        sor = _bare_mysqlor()
        sor.dataChanged = True
        asyncio.run(sor.commit())
        self.assertEqual(sor.conn.committed, 1)
        self.assertFalse(sor.dataChanged, "commit 后 dataChanged 仍为 True（拼写 bug 未修）")

    def test_rollback_resets_dataChanged(self):
        sor = _bare_mysqlor()
        sor.dataChanged = True
        asyncio.run(sor.rollback())
        self.assertFalse(sor.dataChanged)


class TestDataChangedCoverage(unittest.TestCase):
    """ddl/call/replace 这类会改数据的语句也必须置 dataChanged（2026-11-01 复审）。"""

    def test_non_select_sets_dataChanged(self):
        from sqlor.sor import SQLor

        class S(SQLor):
            def getSqlType(self, sql):
                return 'dml' if sql.lower().startswith(('insert', 'update', 'delete')) else 'ddl'

            def cursor(self):
                return object()

            async def runVarSQL(self, cur, sql, value):
                return 1

        s = object.__new__(S)
        s.dataChanged = False
        for sql in ("call sp_sync()", "replace into t values(1)", "create table x(a int)"):
            s.dataChanged = False
            asyncio.run(s.execute(sql, {}))
            self.assertTrue(s.dataChanged, f"{sql!r} 改数据却没置 dataChanged → 出口会 rollback 丢写")


class TestPoolConcurrencyRegression(unittest.TestCase):
    """③ 并发获取必须拿到不同连接；空闲回收不得关闭在用连接。"""

    def _pool_with_one_idle(self, slow=0.05):
        from sqlor.dbpools import SqlorPool

        created = []

        async def create():
            class S:
                def __init__(self):
                    self.id = len(created)
                    self.entered = 0

                async def connect(self):
                    pass

                async def enter(self):
                    self.entered += 1

                async def exit(self):
                    self.entered -= 1

                async def close(self):
                    pass

            s = S()
            created.append(s)
            return s

        pool = SqlorPool(create, maxconn=5)

        async def slow_check(_sor):
            await asyncio.sleep(slow)     # 放大 check-then-await 的窗口
            return True

        pool._check_alive = slow_check
        return pool, created

    def test_concurrent_acquire_returns_distinct_connections(self):
        from sqlor.dbpools import SqlorPool  # noqa: F401
        from appPublic.dictObject import DictObject
        import time as _t

        pool, created = self._pool_with_one_idle()
        # 预置一条空闲连接（use_at 设老，强制走 await 探活分支）
        pool.sqlors.append(DictObject(used=False, use_at=_t.time() - 60,
                                      sqlor=types.SimpleNamespace()))

        async def two():
            async def grab():
                async with pool.context() as s:
                    await asyncio.sleep(0.02)   # 持有期间另一个协程来抢
                    return id(s)
            return await asyncio.gather(grab(), grab())
        a, b = asyncio.run(two())
        self.assertNotEqual(a, b, "两个协程拿到了同一个连接对象（check-then-await 竞态）")
        self.assertEqual(len(created), 1, "预置连接应被第一个协程复用，第二个协程才新建")

    def test_cancel_during_check_alive_releases_reservation(self):
        """探活被取消（长任务超时/取消的高频路径）→ 预留标记必须复位，不得永久泄漏。"""
        from sqlor.dbpools import SqlorPool
        from appPublic.dictObject import DictObject
        import time as _t

        async def create():
            return types.SimpleNamespace()

        pool = SqlorPool(create, maxconn=5)
        entry = DictObject(used=False, use_at=_t.time() - 60, sqlor=types.SimpleNamespace())
        pool.sqlors.append(entry)

        async def cancel_check(_sor):
            raise asyncio.CancelledError()

        pool._check_alive = cancel_check

        async def run():
            async with pool.context() as _s:
                pass

        with self.assertRaises(asyncio.CancelledError):
            asyncio.run(run())
        self.assertFalse(entry.used, "探活被取消后条目仍 used=True —— 连接永久泄漏")

    def test_cleanup_idle_skips_in_use(self):
        from sqlor.dbpools import SqlorPool
        from appPublic.dictObject import DictObject
        import time as _t

        async def create():
            return types.SimpleNamespace()

        pool = SqlorPool(create, maxconn=5)
        entry = DictObject(used=True, use_at=_t.time() - 9999, sqlor=types.SimpleNamespace())
        pool.sqlors.append(entry)
        pool._cleanup_idle()
        self.assertIn(entry, pool.sqlors, "空闲回收把正在使用的连接删掉了")


class TestDriverConsistency(unittest.TestCase):
    """④ 各驱动子类的 close 都应是协程函数，避免某驱动退回同步实现。"""

    def test_all_drivers_close_is_coroutine(self):
        import inspect as _inspect
        mods = [
            ('sqlor.mysqlor', 'MySqlor'),
            ('sqlor.aiosqliteor', 'Aiosqliteor'),
            ('sqlor.pgsqlor', 'PgSqlor'),
            ('sqlor.mssqlor', 'MsSqlor'),
        ]
        for mod, cls in mods:
            with self.subTest(driver=cls):
                try:
                    m = __import__(mod, fromlist=[cls])
                    k = getattr(m, cls)
                except Exception as e:        # 驱动依赖缺失则跳过该驱动，不影响其它
                    self.skipTest(f"{cls} 导入失败（依赖缺失）：{e}")
                    continue
                self.assertTrue(_inspect.iscoroutinefunction(k.close),
                                f"{cls}.close 不是协程函数")


if __name__ == '__main__':
    unittest.main(verbosity=2)
