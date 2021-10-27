import asyncio
from selectors import DefaultSelector
from typing import Any
from typing import Callable
from typing import Optional


# From https://github.com/spulec/freezegun/issues/290
class TimeForwardSelector(DefaultSelector):
    def __init__(self, *, on_idle: Callable[[], None]) -> None:
        super().__init__()
        self._current_time: float = 0
        self._on_idle = on_idle

    def select(self, timeout: Optional[float] = None) -> Any:
        # There are tasks to be scheduled. Continue simulating.
        self._current_time += timeout or 0
        events = DefaultSelector.select(self, 0)
        if not events and timeout is None:
            self._on_idle()
        return events


class TimeForwardLoop(asyncio.SelectorEventLoop):  # type: ignore
    _selector: TimeForwardSelector

    def __init__(self) -> None:
        super().__init__(selector=TimeForwardSelector(on_idle=self.on_idle))
        self._idled: Optional[asyncio.Event] = None

    def time(self) -> float:
        return self._selector._current_time

    def on_idle(self) -> None:
        self.idled.set()

    @property
    def idled(self) -> asyncio.Event:
        if self._idled is None:
            self._idled = asyncio.Event()
        return self._idled

    async def wait_idle(self) -> None:
        await self.idled.wait()
        self.idled.clear()
