import typing as t

import asyncio
from unittest.mock import Mock

from saturn_engine.utils.asyncutils import DelayedThrottle


async def test_delayed_task() -> None:
    func_mock = Mock()
    func_wait = asyncio.Event()

    async def func(*args: t.Any, **kwargs: t.Any) -> None:
        try:
            func_mock(*args, **kwargs)
            await func_wait.wait()
        except BaseException as e:
            func_mock(error=e)
            raise

    delayed_func = DelayedThrottle(func, delay=5)

    # The call is delayed and done with the latest parameters.
    func_wait.set()
    delayed_func(1, a="b")
    await asyncio.sleep(4)
    delayed_func(2, a="b")
    func_mock.assert_not_called()

    await asyncio.sleep(2)
    func_mock.assert_called_once_with(2, a="b")
    func_mock.reset_mock()
    await asyncio.sleep(6)
    func_mock.assert_not_called()

    # A call can get cancelled, it won't be called
    func_wait.clear()
    func_mock.reset_mock()

    delayed_func()
    await asyncio.sleep(4)
    await delayed_func.cancel()
    func_mock.assert_not_called()
    await asyncio.sleep(2)
    func_mock.assert_not_called()

    # A call can get cancelled while its being called
    func_wait.clear()
    func_mock.reset_mock()

    delayed_func()
    await asyncio.sleep(6)
    func_mock.assert_called_once()
    func_mock.reset_mock()
    await delayed_func.cancel()
    func_mock.assert_called_once()
    assert isinstance(func_mock.call_args.kwargs["error"], asyncio.CancelledError)

    # Can also cancel while nothing is running.
    await delayed_func.cancel()

    # A pending call can be "flush"ed, calling it right away.
    func_wait.set()
    func_mock.reset_mock()

    await delayed_func.flush()
    func_mock.assert_not_called()

    delayed_func()
    await delayed_func.flush()
    func_mock.assert_called_once()
    func_mock.reset_mock()

    await asyncio.sleep(6)
    func_mock.assert_not_called()
    await delayed_func.flush()
    func_mock.assert_not_called()

    # When a new call happen while a task is running, the new call is going to
    # be delayed too.
    func_wait.clear()
    func_mock.reset_mock()

    delayed_func(1)
    await asyncio.sleep(6)
    func_mock.assert_called_once_with(1)
    func_mock.reset_mock()
    delayed_func(2)
    func_wait.set()

    await asyncio.sleep(6)
    func_mock.assert_called_once_with(2)
    func_mock.reset_mock()
    await asyncio.sleep(6)
    func_mock.assert_not_called()

    # When a new call happen while a task is running, the new call is going to
    # be cancelled too.
    func_wait.clear()
    func_mock.reset_mock()

    delayed_func(1)
    await asyncio.sleep(6)
    func_mock.assert_called_once_with(1)
    func_mock.reset_mock()
    delayed_func(2)

    await delayed_func.cancel()
    await asyncio.sleep(6)
    assert isinstance(func_mock.call_args.kwargs["error"], asyncio.CancelledError)
    func_mock.reset_mock()
    func_wait.set()

    delayed_func(3)
    await asyncio.sleep(6)
    func_mock.assert_called_once_with(3)
