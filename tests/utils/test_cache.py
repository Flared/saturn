import threading
from unittest.mock import Mock
from unittest.mock import call

from saturn_engine.utils.cache import threadsafe_cache


def test_threadsafe_lock() -> None:
    in_func = threading.Event()
    out_func = threading.Event()
    spy = Mock()
    results = []

    @threadsafe_cache
    def func(x: int) -> int:
        spy(x)
        in_func.set()

        # only block for x==1, so we can test other x value
        if x == 1:
            out_func.wait()
        return x

    def check_func(x: int) -> None:
        results.append(func(x))

    t1 = threading.Thread(target=check_func, args=(1,))
    t1.daemon = True
    t1.start()

    # Wait until t1 is in the cached function.
    in_func.wait()
    assert t1.is_alive()

    in_func.clear()
    t2 = threading.Thread(target=check_func, args=(1,))
    t2.daemon = True
    t2.start()
    # Give some time for t2 to reach the cache lock.
    assert not in_func.wait(0.1)

    # Using a different key won't block on the same lock.
    assert func(2) == 2

    # Unblock t1
    out_func.set()
    t1.join()
    t2.join()

    # Both threads returned the x value.
    assert results == [1, 1]
    # It now use the cached value (in_func event is still blocking)
    assert func(1) == 1

    # Each key should only have been called once.
    assert spy.call_args_list == [call(1), call(2)]
