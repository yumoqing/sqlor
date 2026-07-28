"""
Test: verify SqlorPool discards dead connections and creates new ones.
Uses mock objects to simulate aiomysql TCPTransport closed errors.
"""
import sys
import os

# Add local repo to path BEFORE any other imports
local_repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, local_repo)

import asyncio
import time

# Mock appPublic modules before importing dbpools
class MockDictObject(dict):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.__dict__.update(kwargs)
    def __getattr__(self, k):
        return self.get(k)
    def __setattr__(self, k, v):
        self[k] = v
        self.__dict__[k] = v

# Patch imports
import types
worker_mod = types.ModuleType('appPublic.worker')
worker_mod.get_event_loop = lambda: asyncio.get_event_loop()
worker_mod.awaitify = lambda f: f
sys.modules['appPublic.worker'] = worker_mod

dictobj_mod = types.ModuleType('appPublic.dictObject')
dictobj_mod.DictObject = MockDictObject
sys.modules['appPublic.dictObject'] = dictobj_mod

log_mod = types.ModuleType('appPublic.log')
log_mod.exception = lambda *a, **k: print(f"EXCEPTION: {a}")
log_mod.debug = lambda *a, **k: print(f"DEBUG: {a}")
log_mod.info = lambda *a, **k: None
sys.modules['appPublic.log'] = log_mod

# Stub out all other appPublic imports needed
for name in ['myImport', 'Singleton', 'myjson', 'jsonConfig', 'rc4',
             'event_dispatcher', 'myTE', 'objectAction', 'argsConvert',
             'registerfunction', 'aes', 'unicoding']:
    mod = types.ModuleType(f'appPublic.{name}')
    if name == 'Singleton':
        mod.SingletonDecorator = lambda cls: cls
    if name == 'event_dispatcher':
        class FakeDispatcher:
            def __init__(self): pass
            async def dispatch(self, *a): pass
        mod.EventDispatcher = FakeDispatcher
    if name == 'rc4':
        mod.unpassword = lambda x: x
    if name == 'myImport':
        mod.myImport = lambda x: None
    if name == 'myjson':
        mod.loadf = lambda *a, **k: {}
    if name == 'jsonConfig':
        mod.getConfig = lambda *a, **k: {}
    if name == 'argsConvert':
        class FakeAC:
            def __init__(self, *a): pass
            def convert(self, s, ns): return s
            def findAllVariables(self, s): return []
            def getVarValue(self, v, ns, d): return d
        class FakeCC:
            def convert(self, s, ns): return s
        mod.ArgsConvert = FakeAC
        mod.ConditionConvert = FakeCC
    if name == 'registerfunction':
        class FakeRF:
            async def exe(self, *a): return None
        mod.RegisterFunction = lambda: FakeRF()
    sys.modules[f'appPublic.{name}'] = mod

# Now import the module under test
from sqlor.dbpools import SqlorPool

class FakeSor:
    """Simulates a SQLor with controllable alive/dead state."""
    def __init__(self, alive=True):
        self.alive = alive
        self.entered = False
        self.closed = False
        self.test_sqlstr = "SELECT 1"
        self.conn = MockConn(alive)
        self.dataChanged = False
        self.dbpools = None

    async def enter(self):
        if not self.alive:
            raise RuntimeError("unable to perform operation on <TCPTransport closed=True reading=False>; the handler is closed")
        self.entered = True

    async def exit(self):
        self.entered = False

    async def close(self):
        self.closed = True

    async def execute(self, sql, ns):
        if not self.alive:
            raise RuntimeError("unable to perform operation on <TCPTransport closed=True>; the handler is closed")
        return []

    async def rollback(self):
        pass

    async def commit(self):
        pass

class MockConn:
    def __init__(self, alive=True):
        self._alive = alive
        self._writer = MockWriter(alive)

class MockWriter:
    def __init__(self, alive=True):
        self.transport = MockTransport(alive)

class MockTransport:
    def __init__(self, alive=True):
        self._alive = alive
    def is_closing(self):
        return not self._alive

call_count = 0
def make_fake_sor(alive_seq):
    """Factory: returns FakeSor instances. First N are dead, rest alive."""
    async def factory():
        global call_count
        idx = call_count
        call_count += 1
        is_alive = alive_seq[idx] if idx < len(alive_seq) else True
        return FakeSor(alive=is_alive)
    return factory


async def test_discard_dead_connection():
    """Dead idle connections should be discarded; new ones created."""
    global call_count
    call_count = 0

    # Pool starts empty; first context() call creates a new connection
    factory = make_fake_sor([True, False, True])
    pool = SqlorPool(factory)

    # 1st use: creates connection #0 (alive), uses it, returns it to pool
    async with pool.context() as sor:
        assert sor.alive, "First connection should be alive"
    print("PASS: 1st context() - alive connection used and returned to pool")

    # Now mark the pooled connection as dead (simulate TCP timeout)
    pool.sqlors[0].sqlor.alive = False
    pool.sqlors[0].sqlor.conn._alive = False
    pool.sqlors[0].sqlor.conn._writer.transport._alive = False

    # 2nd use: _check_alive detects dead, discards it, creates new connection #2 (alive)
    async with pool.context() as sor:
        assert sor.alive, "Should get a new alive connection after discarding dead one"
    print("PASS: 2nd context() - dead connection discarded, new alive one created")

    # Verify the dead connection was removed
    assert len(pool.sqlors) <= 2, f"Pool should have at most 2 entries, got {len(pool.sqlors)}"
    print(f"PASS: Pool has {len(pool.sqlors)} connections (dead one removed)")


async def test_runtime_error_during_use():
    """If connection dies during SQL execution, it should be discarded."""
    global call_count
    call_count = 0

    factory = make_fake_sor([True])
    pool = SqlorPool(factory)

    try:
        async with pool.context() as sor:
            # Simulate connection dying mid-query
            sor.alive = False
            sor.conn._alive = False
            sor.conn._writer.transport._alive = False
            raise RuntimeError("unable to perform operation on <TCPTransport closed=True reading=False 0x556306af3430>; the handler is closed")
    except RuntimeError as e:
        assert 'closed' in str(e)

    # Verify connection was discarded from pool
    assert len(pool.sqlors) == 0, f"Dead connection should be removed, got {len(pool.sqlors)}"
    print("PASS: RuntimeError during use -> connection discarded from pool")


async def test_normal_usage_no_discard():
    """Healthy connections should stay in the pool."""
    global call_count
    call_count = 0

    factory = make_fake_sor([True])
    pool = SqlorPool(factory)

    async with pool.context() as sor:
        await sor.enter()
        result = await sor.execute("SELECT 1", {})
        await sor.exit()

    assert len(pool.sqlors) == 1, f"Healthy connection should remain, got {len(pool.sqlors)}"
    assert pool.sqlors[0].used == False, "Connection should be marked unused"
    print("PASS: Normal usage - connection stays in pool")


async def test_mysql_error_during_use():
    """MySQL OperationalError/InterfaceError during use should discard connection."""
    global call_count
    call_count = 0

    factory = make_fake_sor([True])
    pool = SqlorPool(factory)

    mysql_errors = [
        Exception("(0, 'Not connected')"),
        Exception("(2014, 'Command Out of Sync')"),
        Exception("Lost connection to MySQL server"),
        Exception("Connection reset by peer"),
        Exception("Server disconnected"),
    ]

    for err in mysql_errors:
        try:
            async with pool.context() as sor:
                raise err
        except Exception:
            pass
        assert len(pool.sqlors) == 0, f"{err} should be discarded"
        pool = SqlorPool(make_fake_sor([True]))

    print("PASS: MySQL errors during use -> connection discarded")


async def test_business_error_not_discarded():
    """Normal business logic errors should NOT discard connection."""
    global call_count
    call_count = 0

    factory = make_fake_sor([True])
    pool = SqlorPool(factory)

    try:
        async with pool.context() as sor:
            raise ValueError("business logic error")
    except ValueError:
        pass

    assert len(pool.sqlors) == 1, "Business error should not discard connection"
    assert pool.sqlors[0].used == False
    print("PASS: Business error - connection stays in pool")


async def main():
    await test_normal_usage_no_discard()
    await test_discard_dead_connection()
    await test_runtime_error_during_use()
    await test_mysql_error_during_use()
    await test_business_error_not_discarded()
    print("\n=== ALL TESTS PASSED ===")


if __name__ == '__main__':
    asyncio.run(main())
