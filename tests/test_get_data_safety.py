"""sqlor 取消安全回归测试（2026-11-15）。

背景（生产事故，18 天 143 条）：
  任务日志里出现 `TypeError: 'NoneType' object is not iterable`，位置固定是
  `sor.py::_get_data` 的 `fields = [i[0].lower() for i in cur.description]`。
  同一天里 `aclose(): asynchronous generator is already running` /
  `Task was destroyed but it is pending` 与它一一对应 —— 说明查询在飞行中
  被取消/关闭，而旧实现读的是「开头捕获、随后已被 enter()/exit() 换掉或关掉」
  的那条游标，于是拿到 description=None 的假故障，把真正的取消原因盖掉。

本文件锁死三条不变量（不需要真库、不需要网络）：
  ① 结果必须从 `execute()` 实际使用的那条游标读取（并发 enter() 换游标也不误报）；
  ② description 为 None 时必须抛**明确**异常（含 SQL 与原因），不再抛 TypeError；
  ③ 退出时若仍有查询在飞（生成器悬空），连接标记 `_poisoned` 且池不再复用；
     正常走完的查询则 `_inflight` 归零、不中毒。

跑法：cd <repo> && python3 -m unittest tests.test_get_data_safety -v
"""
import asyncio
import os
import sys
import unittest

_TESTS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _TESTS)
import test_regressions as base          # noqa: E402  复用 hermetic 引导（桩 appPublic/驱动）

from sqlor.mysqlor import MySqlor        # noqa: E402
from sqlor.sor import SQLor              # noqa: E402


class Cur:
    """最小游标桩：description 可设、fetchone 走队列。"""

    def __init__(self, description=(('id',),), rows=None):
        self.description = description
        self._rows = list(rows or [])
        self.closed = False
        self.executed = []

    async def execute(self, sql, args=None):
        self.executed.append(sql)
        return 0

    async def fetchone(self):
        return self._rows.pop(0) if self._rows else None

    async def fetchall(self):
        return []

    async def close(self):
        self.closed = True


class Sor(SQLor):
    """可控 SQLor：cursor() 返回预置游标，runVarSQL 不碰真库。"""

    def __init__(self, cur):
        self.cur = cur
        self._inflight = 0
        self._poisoned = False
        self.executed = []

    def cursor(self):
        return self.cur

    async def runVarSQL(self, cursor, sql, ns):
        self.executed.append(sql)
        await cursor.execute(sql, ns)
        return 0

    def getSqlType(self, sql):
        return 'qry'

    async def exit(self):          # 池丢弃路径会调它（真实实现见 MySqlor.exit）
        self.cur = None

    async def close(self):
        return None


def _drain(sor, sql='select 1'):
    async def go():
        return await sor.sqlExe(sql, {})
    return asyncio.run(go())


class TestCurUsedNotStale(unittest.TestCase):
    """① 结果取自 execute 真正使用的游标（模拟 enter() 在 await 窗口里换游标）。"""

    def test_reads_executed_cursor_after_swap(self):
        first = Cur(description=None)          # 被 enter() 换掉的旧游标
        second = Cur(description=(('id',),), rows=[(1,), (2,)])

        class Swapping(Sor):
            async def execute(self, sql, value):
                # 模拟并发 enter()：self.cur 被换成新游标后再执行
                self.cur = second
                return await super().execute(sql, value)

        sor = Swapping(first)
        rows = _drain(sor, 'select id from t')
        self.assertEqual([r.id for r in rows], [1, 2],
                         '旧实现用开头捕获的 first.description=None 会抛 TypeError')

    def test_clear_error_when_executed_cursor_has_no_resultset(self):
        """② execute 后 description 仍为 None → 明确异常，不再是 'NoneType' not iterable。"""
        sor = Sor(Cur(description=None))
        with self.assertRaises(RuntimeError) as cm:
            _drain(sor, 'select 1 from nowhere')
        msg = str(cm.exception)
        self.assertIn('cursor closed during query', msg)
        self.assertIn('select 1 from nowhere', msg)   # 带 SQL，便于定位

    def test_normal_query_not_poisoned(self):
        """正常查询：_inflight 归零、不中毒、游标不被误关。"""
        cur = Cur(description=(('id',),), rows=[(7,)])
        sor = Sor(cur)
        rows = _drain(sor, 'select id from t')
        self.assertEqual([r.id for r in rows], [7])
        self.assertEqual(sor._inflight, 0)
        self.assertFalse(getattr(sor, '_poisoned', False))

    def test_error_still_decrements_inflight(self):
        """异常路径也要把 _inflight 归零（否则连接永远中毒）。"""
        sor = Sor(Cur(description=None))
        with self.assertRaises(RuntimeError):
            _drain(sor)
        self.assertEqual(sor._inflight, 0)


class TestExitPoisonsInflight(unittest.TestCase):
    """③ 退出时仍有查询在飞 → 中毒；正常退出 → 不中毒。"""

    def test_exit_marks_poisoned(self):
        sor = object.__new__(MySqlor)
        sor.cur = Cur()
        sor._inflight = 2                     # 模拟两个悬空生成器
        asyncio.run(sor.exit())
        self.assertTrue(getattr(sor, '_poisoned', False), '在飞查询未把连接标记为中毒')
        self.assertIsNone(sor.cur)

    def test_exit_clean_when_no_inflight(self):
        sor = object.__new__(MySqlor)
        sor.cur = Cur()
        sor._inflight = 0
        asyncio.run(sor.exit())
        self.assertFalse(getattr(sor, '_poisoned', False))

    def test_enter_resets_flags(self):
        from test_regressions import FakeConn
        sor = object.__new__(MySqlor)
        sor.conn = FakeConn()
        sor._poisoned = True
        sor._inflight = 3

        async def _fake_cursor(*a, **k):
            return Cur()

        sor.conn.cursor = _fake_cursor
        asyncio.run(sor.enter())
        self.assertFalse(sor._poisoned)
        self.assertEqual(sor._inflight, 0)


class TestPoolDiscardsPoisoned(unittest.TestCase):
    """池选择逻辑：中毒连接直接丢弃，绝不交付给下一个请求。"""

    def test_poisoned_entry_discarded(self):
        from sqlor.dbpools import SqlorPool
        pool = SqlorPool(lambda: None)
        good_sqlor = Sor(Cur())
        poisoned = Sor(Cur())
        poisoned._poisoned = True
        pool.sqlors = [_entry(poisoned), _entry(good_sqlor)]
        picked = asyncio.run(_pick(pool))
        self.assertIs(picked, good_sqlor, '中毒连接被交付了')
        self.assertEqual(len(pool.sqlors), 1)
        self.assertIs(pool.sqlors[0].sqlor, good_sqlor)


class _E(dict):
    def __init__(self, sqlor):
        super().__init__(used=False, use_at=0.0, sqlor=sqlor)
        self.__dict__.update(self)

    def __getattr__(self, k):
        return self[k]

    def __setattr__(self, k, v):
        self[k] = v
        self.__dict__[k] = v


def _entry(sqlor):
    return _E(sqlor)


async def _pick(pool):
    import time
    # 连接刚用过（不走 _cleanup_idle、也不做 DB 往返探活），只验证「中毒→丢弃」这条分支
    for e in pool.sqlors:
        e.use_at = time.time()
    pool.CHECK_ALIVE_THRESHOLD = 999
    pool._transport_ok = lambda s: True
    async with pool.context() as sqlor:
        return sqlor


if __name__ == '__main__':
    unittest.main(verbosity=2)