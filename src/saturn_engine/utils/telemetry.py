import typing as t

import time
from functools import wraps
from types import TracebackType

from opentelemetry.metrics import Histogram
from opentelemetry.util import types

CallableT = t.TypeVar("CallableT", bound=t.Callable)


# Adapted from https://github.com/open-telemetry/opentelemetry-python/pull/2827
class Timer:
    def __init__(self, histogram: Histogram) -> None:
        super().__init__()
        self._start: t.Optional[float] = None
        self._attributes: t.Optional[dict[str, t.Any]] = None
        self._histogram: Histogram = histogram

    def __enter__(self) -> "Timer":
        self._start = time.perf_counter()
        return self

    def __exit__(
        self,
        exc_type: t.Optional[t.Type[BaseException]],
        exc_val: t.Optional[BaseException],
        exc_tb: t.Optional[TracebackType],
    ) -> None:
        if self._start is None:
            raise ValueError("Timer not started")
        duration = max(round((time.perf_counter() - self._start) * 1000), 0)
        self._histogram.record(duration, self._attributes)

    def time(self, attributes: types.Attributes = None) -> "Timer":
        self._attributes = dict(attributes) if attributes else None
        return self

    def set_attributes(self, attributes: t.Mapping[str, types.AttributeValue]) -> None:
        if self._attributes is None:
            self._attributes = dict(attributes)
        else:
            self._attributes.update(attributes)

    def __call__(self, func: CallableT) -> CallableT:
        @wraps(func)
        def wrapped(*args: t.Any, **kwargs: t.Any) -> t.Any:
            with self.time(attributes=self._attributes):
                return func(*args, **kwargs)

        return t.cast(CallableT, wrapped)


def get_timer(histogram: Histogram) -> Timer:
    return Timer(histogram)
