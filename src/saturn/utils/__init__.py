import enum
from functools import wraps
from typing import Callable
from typing import Iterable
from typing import Iterator
from typing import Optional
from typing import TypeVar

T = TypeVar("T")


def lazy(init: Callable[[], T]) -> Callable[[], T]:
    """
    Ensure a function is called only once. Useful to lazilly setup some global.

    >>> def expansive_computation(): ...
    >>> @lazy
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
    value: Optional[T] = None

    @wraps(init)
    def wrapper() -> T:
        nonlocal value
        if value is None:
            value = init()
        return value

    return wrapper


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
