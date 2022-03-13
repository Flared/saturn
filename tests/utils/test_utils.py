from typing import Any

import asyncio
from unittest.mock import AsyncMock

import pytest

from saturn_engine.utils import get_own_attr
from saturn_engine.utils import has_own_attr
from saturn_engine.utils import lazy
from saturn_engine.utils.asyncutils import DelayedThrottle


def test_get_own_attr() -> None:
    own = object()
    notown = object()
    default = object()

    class A:
        a = notown

    class B:
        __slots__ = ["a"]
        a: Any
        b = notown

    a = A()
    assert get_own_attr(a, "a", default) is default
    assert get_own_attr(a, "b", default) is default
    with pytest.raises(AttributeError):
        assert get_own_attr(a, "a")
    with pytest.raises(AttributeError):
        assert get_own_attr(a, "b")
    assert has_own_attr(a, "a") is False
    assert has_own_attr(a, "b") is False
    a.a = own
    assert get_own_attr(a, "a", default) is own
    assert get_own_attr(a, "a") is own
    assert has_own_attr(a, "a") is True
    assert has_own_attr(a, "b") is False

    b = B()
    assert get_own_attr(b, "a", default) is default
    assert get_own_attr(b, "b", default) is default
    assert get_own_attr(b, "c", default) is default
    with pytest.raises(AttributeError):
        assert get_own_attr(b, "a")
    with pytest.raises(AttributeError):
        assert get_own_attr(b, "b")
    with pytest.raises(AttributeError):
        assert get_own_attr(b, "c")
    assert has_own_attr(b, "a") is False
    assert has_own_attr(b, "b") is False
    assert has_own_attr(b, "c") is False
    b.a = own
    assert get_own_attr(b, "a", default) is own
    assert get_own_attr(b, "a") is own
    assert has_own_attr(b, "a") is True
    assert has_own_attr(b, "b") is False
    assert has_own_attr(b, "c") is False


@pytest.mark.asyncio
async def test_delayed_throttle() -> None:
    mock = AsyncMock()
    mock.__qualname__ = "mock"
    throttle = DelayedThrottle(mock, delay=1.0)
    await throttle(1, a=2)
    await asyncio.sleep(0.9)
    await throttle(2, a=3)
    mock.assert_not_awaited()
    await asyncio.sleep(0.2)
    mock.assert_awaited_once_with(2, a=3)
    mock.reset_mock()

    await throttle(3, b=4)
    await asyncio.sleep(0.9)
    mock.assert_not_awaited()
    await asyncio.sleep(0.2)
    mock.assert_awaited_once_with(3, b=4)
    mock.reset_mock()

    await throttle(4, b=5)
    await asyncio.sleep(0.9)
    mock.assert_not_awaited()
    await throttle.cancel()
    await asyncio.sleep(0.2)
    mock.assert_not_awaited()
    mock.reset_mock()


def test_lazy() -> None:
    count: int = 0

    @lazy()
    def compute() -> str:
        nonlocal count
        count = count + 1
        return "aa"

    assert compute() == "aa"
    assert count == 1

    assert compute() == "aa"
    assert count == 1

    compute.clear()
    assert compute() == "aa"
    assert count == 2
