import enum
import threading
from functools import wraps
from typing import Any
from typing import Callable
from typing import Iterable
from typing import Iterator
from typing import TypeVar
from typing import Union

T = TypeVar("T")


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
