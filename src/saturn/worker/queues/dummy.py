import asyncio
from datetime import datetime
from typing import Optional

from . import Queue


class DummyQueue(Queue[str]):
    """A dummy queue that yield a message every second"""

    def __init__(self) -> None:
        self.last_yield_at = datetime.now()

    def get_nowait(self) -> Optional[str]:
        now = datetime.now()
        if (now - self.last_yield_at).total_seconds() > 1:
            self.last_yield_at = now
            return "hello"
        return None

    async def get(self) -> str:
        now = datetime.now()
        await asyncio.sleep((now - self.last_yield_at).total_seconds())
        self.last_yield_at = now
        return "hello"
