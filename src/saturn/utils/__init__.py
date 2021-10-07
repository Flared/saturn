from functools import wraps
from typing import Callable
from typing import Optional
from typing import TypeVar

T = TypeVar("T")


def lazy(init: Callable[[], T]) -> Callable[[], T]:
    value: Optional[T] = None

    @wraps(init)
    def wrapper() -> T:
        nonlocal value
        if value is None:
            value = init()
        return value

    return wrapper
