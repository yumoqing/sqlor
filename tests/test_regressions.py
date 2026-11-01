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

# ── 让 sqlor 在无 appPublic 的环境也能 import（有的话用真的）──
try:                                       # pragma: no cover
    import appPublic  # noqa: F401
except Exception:                          # pragma: no cover
    _ap = types.ModuleType('appPublic')
    _wr = types.ModuleType('appPublic.worker')
    _wr.get_event_loop = lambda: asyncio.get_event_loop()
    _wr.awaitify = lambda f: f
    _do = types.ModuleType('appPublic.dictObject')

    class _DictObject(dict):
        def __init__(self, **kw):
            super().__init__(**kw)
            self.__dict__.update(kw)

        def __getattr__(self, k):
            return self.get(k)

        def __setattr__(self, k, v):
            self[k] = v
            self.__dict__[k] = v

    _do.DictObject = _DictObject
    _lg = types.ModuleType('appPublic.log')
    _lg.exception = _lg.debug = _lg.info = _lg.warning = lambda *a, **k: None
    _ac = types.ModuleType('appPublic.argsConvert')
    _ac.ArgsConvert = _ac.ConditionConvert = object
    sys.modules.update({
        'appPublic': _ap, 'appPublic.worker': _wr, 'appPublic.dictObject': _do,
        'appPublic.log': _lg, 'appPublic.argsConvert': _ac,
    })


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
        pool.sqlors.append(DictObject(used=False, use_at=_t.time() - 999,
                                      sqlor=types.SimpleNamespace()))

        async def two():
            async def grab():
                async with pool.context() as s:
                    await asyncio.sleep(0.02)   # 持有期间另一个协程来抢
                    return id(s)
            return await asyncio.gather(grab(), grab())
        a, b = asyncio.run(two())
        self.assertNotEqual(a, b, "两个协程拿到了同一个连接对象（check-then-await 竞态）")

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
            try:
                m = __import__(mod, fromlist=[cls])
                k = getattr(m, cls)
            except Exception as e:            # 驱动依赖缺失则跳过
                self.skipTest(f"{cls} 导入失败（依赖缺失）：{e}")
                continue
            self.assertTrue(_inspect.iscoroutinefunction(k.close),
                            f"{cls}.close 不是协程函数")


if __name__ == '__main__':
    unittest.main(verbosity=2)
