import typing as t

import functools
import threading
from collections import defaultdict


class _CacheLock:
    __slots__ = ("lock",)

    def __init__(self) -> None:
        self.lock = threading.Lock()


def threadsafe_cache(func: t.Callable) -> t.Callable:
    """
    Same as functools, but with a lock to ensure function are called only
    once per key.
    """

    cache: dict = defaultdict(_CacheLock)

    def wrapper(*args: t.Any, **kwargs: t.Any) -> None:
        key = functools._make_key(args, kwargs, typed=False)
        result = cache[key]
        if isinstance(result, _CacheLock):
            with result.lock:
                result = cache[key]
                if isinstance(result, _CacheLock):
                    result = func(*args, **kwargs)
                    cache[key] = result
        return result

    return wrapper
