import typing as t

from collections import OrderedDict

K = t.TypeVar("K")
V = t.TypeVar("V")


class LRUDefaultDict(OrderedDict[K, V]):
    """Dict with a limited length, ejecting LRUs as needed."""

    def __init__(
        self,
        cache_len: int = 10,
        default_factory: t.Callable[[], V] | None = None,
        *args: list[t.Any],
        **kwargs: dict[str, t.Any]
    ):
        if cache_len <= 0:
            raise ValueError("cache_len must be greater than 0")
        self.cache_len = cache_len
        self.default_factory = default_factory

        super().__init__(*args, **kwargs)

    def __setitem__(self, key: K, value: V) -> None:
        super().__setitem__(key, value)
        super().move_to_end(key)

        while len(self) > self.cache_len:
            oldkey = next(iter(self))
            super().__delitem__(oldkey)

    def __getitem__(self, key: K) -> V:
        try:
            val = super().__getitem__(key)
        except KeyError:
            if not self.default_factory:
                raise
            val = self.default_factory()
            self[key] = val
        super().move_to_end(key)

        return val
