import typing as t

import builtins
import collections
import enum
import threading
from collections.abc import Iterable
from collections.abc import Iterator
from collections.abc import Mapping
from contextlib import asynccontextmanager
from datetime import datetime
from datetime import timezone
from functools import wraps

T = t.TypeVar("T")


class Sentinel(enum.Enum):
    sentinel = object()


MISSING = Sentinel.sentinel


# Some magic number used here and there.
LONG_TIMEOUT = 60
MEDIUM_TIMEOUT = 10


class Scope:
    value: t.Any


class _Lazy(t.Generic[T]):
    def __init__(
        self,
        *,
        scope: t.Union[Scope, threading.local],
        init: t.Callable[[], T],
    ) -> None:
        self.scope = scope
        self.init = init

    def __call__(self) -> T:
        if not hasattr(self.scope, "value"):
            self.scope.value = self.init()
        return self.scope.value

    def clear(self) -> None:
        if hasattr(self.scope, "value"):
            del self.scope.value


def lazy(
    *,
    threadlocal: bool = False,
) -> t.Callable[[t.Callable[[], T]], _Lazy[T]]:
    """
    Ensure a function is called only once. Useful to lazilly setup some global.

    >>> def expansive_computation(): ...
    >>> @lazy()
    ... def say_hi_once():
    ...     print("hi")
    ...     expansive_computation()
    ...     return 1
    ...
    >>> say_hi_once()
    hi
    1
    >>> say_hi_once()
    1
    >>> say_hi_once.clear()
    >>> say_hi_once()
    hi
    1
    """

    scope: t.Union[Scope, threading.local]
    if threadlocal:
        scope = threading.local()
    else:
        scope = Scope()

    def decorator(init: t.Callable[[], T]) -> _Lazy[T]:
        return wraps(init)(
            _Lazy[T](
                init=init,
                scope=scope,
            ),
        )

    return decorator


def flatten(xs: Iterable[Iterable[T]]) -> Iterator[T]:
    """
    Flatten iterable of iterable into list.

    >>> list(flatten([[1, 2], [3, 4]]))
    [1, 2, 3, 4]
    """
    return (item for sublist in xs for item in sublist)


class StrEnum(str, enum.Enum):
    def __str__(self) -> str:
        return self


def get_own_attr(inst: object, attr: str, default: t.Union[T, Sentinel] = MISSING) -> T:
    """
    Act like `getattr`, but only check the instance namespace.

    >>> class A:
    ...     x = 1
    ...     def __init__(self): self.y = 1
    ...
    >>> get_own_attr(A(), 'x', None)
    >>> get_own_attr(A(), 'y')
    1
    """
    try:
        if hasattr(inst, "__slots__"):
            if attr not in inst.__slots__:  # type: ignore[attr-defined]
                raise AttributeError(attr)
            return getattr(inst, attr)

        return inst.__dict__[attr]
    except (AttributeError, KeyError):
        if default is not MISSING:
            return default
        raise AttributeError(attr) from None


def has_own_attr(inst: object, attr: str) -> bool:
    """
    Act like `hasattr`, but only check the instance namespace.

    >>> class A:
    ...     x = 1
    ...     def __init__(self): self.y = 1
    ...
    >>> has_own_attr(A(), 'x')
    False
    >>> has_own_attr(A(), 'y')
    True
    """
    try:
        get_own_attr(inst, attr)
        return True
    except AttributeError:
        return False


def urlcat(*args: str) -> str:
    """
    Like urljoin, without all the footguns.

    >>> urlcat("http://foo.com/", "/biz", "baz", "buz")
    'http://foo.com/biz/baz/buz'
    """
    return "/".join(s.strip("/") for s in args)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def default_utc(date: datetime) -> datetime:
    if date.tzinfo is None:
        return date.replace(tzinfo=timezone.utc)
    return date


class Namespace(collections.UserDict):
    def __getattr__(self, name: str) -> object:
        try:
            return self.data[name]
        except KeyError:
            raise AttributeError(name) from None


class CINamespace(collections.UserDict):
    def __getattr__(self, name: str) -> object:
        try:
            return self.data[name.lower()]
        except KeyError:
            raise AttributeError(name) from None

    def __getitem__(self, name: str) -> object:
        return self.data[name.lower()]

    def __setitem__(self, name: str, value: t.Any) -> None:
        self.data[name.lower()] = value


def assert_never(x: t.NoReturn) -> t.NoReturn:
    raise AssertionError("Unhandled type: {}".format(type(x).__name__))


ExceptionGroup: t.Type

if not (ExceptionGroup := getattr(builtins, "ExceptionGroup", None)):  # type: ignore

    class ExceptionGroup(Exception):  # type: ignore[no-redef]
        def __init__(self, msg: str, errors: list[Exception]) -> None:
            super().__init__(msg)
            self.errors = errors


def deep_merge(*dicts: Mapping) -> dict:
    merged: dict = {}
    for d in dicts:
        for k, v in d.items():
            mv = merged.get(k, ...)
            if mv is not ... and isinstance(v, Mapping) and isinstance(mv, Mapping):
                v = deep_merge(mv, v)
            merged[k] = v
    return merged


@asynccontextmanager
async def sync_context(cm: t.ContextManager[T]) -> t.AsyncIterator[T]:
    with cm as value:
        yield value
