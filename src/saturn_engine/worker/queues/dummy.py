import asyncio
from datetime import datetime

from saturn_engine.utils.log import getLogger

from . import Queue


class DummyQueue(Queue[str]):
    """A dummy queue that yield a message every second"""

    def __init__(self, id: str) -> None:
        self.id = id
        self.logger = getLogger(__name__, self)
        self.last_yield_at = datetime.now()

    async def get(self) -> str:
        self.logger.info("get/before_sleep [q=%s]", self.id)
        await asyncio.sleep(1)
        self.logger.info("get/after_sleep [q=%s]", self.id)
        return f"hello - {self.id}"
