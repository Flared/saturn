import asyncio
from selectors import DefaultSelector
from typing import Any
from typing import Optional


# From https://github.com/spulec/freezegun/issues/290
class TimeForwardSelector(DefaultSelector):
    _current_time: float

    def __init__(self) -> None:
        super().__init__()
        self._current_time = 0

    def select(self, timeout: Optional[float] = None) -> Any:
        # There are tasks to be scheduled. Continue simulating.
        self._current_time += timeout or 0
        return DefaultSelector.select(self, 0)


class TimeForwardLoop(asyncio.SelectorEventLoop):  # type: ignore
    _selector: TimeForwardSelector

    def __init__(self) -> None:
        super().__init__(selector=TimeForwardSelector())

    def time(self) -> float:
        return self._selector._current_time
