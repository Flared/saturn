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


def tri_split(s1: set[T], s2: set[T]) -> tuple[set[T], set[T], set[T]]:
    """
    Return a 3-items tuple of left, intersection and right of two sets.

    >>> tri_split({1, 2}, {2, 3})
    ({1}, {2}, {3})
    """
    left = s1 - s2
    right = s2 - s1
    intersection = s2 & s1
    return left, intersection, right


def flatten(xs: Iterable[Iterable[T]]) -> Iterator[T]:
    """
    Flatten iterable of iterable into list.

    >>> list(flatten([[1, 2], [3, 4]]))
    [1, 2, 3, 4]
    """
    return (item for sublist in xs for item in sublist)
