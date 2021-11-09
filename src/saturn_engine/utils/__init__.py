import asyncio
import contextlib
import enum
import threading
from collections.abc import AsyncGenerator
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Iterable
from collections.abc import Iterator
from datetime import datetime
from datetime import timezone
from functools import wraps
from typing import Any
from typing import Callable
from typing import Generic
from typing import Optional
from typing import TypeVar
from typing import Union

from saturn_engine.utils.log import getLogger

T = TypeVar("T")

AsyncFNone = TypeVar("AsyncFNone", bound=Callable[..., Awaitable[None]])


class Sentinel(enum.Enum):
    sentinel = object()


MISSING = Sentinel.sentinel


class Scope:
    value: Any


F_NOARGS = Callable[[], T]


def lazy(
    *,
    threadlocal: bool = False,
) -> Callable[[F_NOARGS], F_NOARGS]:
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
    """

    scope: Union[Scope, threading.local]
    if threadlocal:
        scope = threading.local()
    else:
        scope = Scope()

    def decorator(init: F_NOARGS) -> F_NOARGS:
        @wraps(init)
        def wrapper() -> T:
            if not hasattr(scope, "value"):
                scope.value = init()
            return scope.value

        return wrapper

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


def get_own_attr(inst: object, attr: str, default: Union[T, Sentinel] = MISSING) -> T:
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
            if attr not in inst.__slots__:
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


async def aiter2agen(iterator: AsyncIterator[T]) -> AsyncGenerator[T, None]:
    """
    Convert an async iterator into an async generator.
    """
    async for x in iterator:
        yield x


class TasksGroup:
    def __init__(self, tasks: Iterable[asyncio.Task] = ()) -> None:
        self.logger = getLogger(__name__, self)
        self.tasks = set(tasks)
        self.updated = asyncio.Event()
        self.updated_task = asyncio.create_task(self.updated.wait())

    def add(self, task: asyncio.Task) -> None:
        self.tasks.add(task)
        self.updated.set()

    def remove(self, task: asyncio.Task) -> None:
        self.tasks.discard(task)
        self.updated.set()

    async def wait(self) -> set[asyncio.Task]:
        self.updated.clear()
        done, _ = await asyncio.wait(
            self.tasks | {self.updated_task}, return_when=asyncio.FIRST_COMPLETED
        )

        if self.updated_task in done:
            done.remove(self.updated_task)
            self.updated_task = asyncio.create_task(self.updated.wait())

        return done

    async def close(self) -> None:
        self.updated_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self.updated_task

        for task in self.tasks:
            task.cancel()
        tasks_results = await asyncio.gather(*self.tasks, return_exceptions=True)
        for result in tasks_results:
            if isinstance(result, Exception):
                self.logger.error(
                    "Task '%s' cancelled with error", task, exc_info=result
                )
        self.tasks.clear()

    def all(self) -> set[asyncio.Task]:
        return self.tasks


def urlcat(*args: str) -> str:
    """
    Like urljoin, without all the footguns.

    >>> urlcat("http://foo.com/", "/biz", "baz", "buz")
    'http://foo.com/biz/baz/buz'
    """
    return "/".join(s.strip("/") for s in args)


class DelayedThrottle(Generic[AsyncFNone]):
    __call__: AsyncFNone

    def __init__(self, func: AsyncFNone, *, delay: float) -> None:
        self.func = func
        self.delay = delay
        self.delayed_task: Optional[asyncio.Task] = None
        self.delayed_lock = asyncio.Lock()
        self.delayed_args: tuple[Any, ...] = ()
        self.delayed_kwargs: dict[str, Any] = {}

    async def __call__(self, *args: Any, **kwargs: Any) -> None:  # type: ignore
        async with self.delayed_lock:
            self.delayed_args = args
            self.delayed_kwargs = kwargs

            if self.delayed_task is None:
                self.delayed_task = asyncio.create_task(self._delay_call())

    async def _delay_call(self) -> None:
        await asyncio.sleep(self.delay)
        async with self.delayed_lock:
            await self.func(*self.delayed_args, **self.delayed_kwargs)
            self.delayed_task = None

    async def cancel(self) -> None:
        if self.delayed_task is None:
            return
        self.delayed_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self.delayed_task


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def default_utc(date: datetime) -> datetime:
    if date.tzinfo is None:
        return date.replace(tzinfo=timezone.utc)
    return date
