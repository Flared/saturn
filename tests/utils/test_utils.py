from typing import Any

import pytest

from saturn_engine.utils import deep_merge
from saturn_engine.utils import get_own_attr
from saturn_engine.utils import has_own_attr
from saturn_engine.utils import lazy


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


def test_deep_merge() -> None:
    assert deep_merge(
        {
            "a": 1,
            "b": 0,
            "c": {"x": 1, "z": 3},
        },
        {
            "b": {"x": 1},
            "c": {"y": 2, "z": 0},
        },
        {
            "d": "foo",
        },
    ) == {"a": 1, "b": {"x": 1}, "c": {"x": 1, "y": 2, "z": 0}, "d": "foo"}
