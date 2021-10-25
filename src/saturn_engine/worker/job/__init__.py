from typing import Optional

from saturn_engine.core import Message
from saturn_engine.utils.log import getLogger
from saturn_engine.utils.options import OptionsSchema

from ..inventories import Inventory
from ..inventories import Item
from ..queues import Publisher


class JobStore(OptionsSchema):
    async def load_cursor(self) -> Optional[int]:
        pass

    async def save_cursor(self, *, after: int) -> None:
        pass


class Job:
    def __init__(
        self, *, inventory: Inventory, publisher: Publisher, store: JobStore
    ) -> None:
        self.logger = getLogger(__name__, self)
        self.inventory = inventory
        self.publisher = publisher
        self.store = store
        self.after: Optional[int] = None

    async def push_batch(self, items: list[Item]) -> None:
        try:
            for item in items:
                if self.after is not None and item.id <= self.after:
                    self.logger.error("Unordered items: %s <= %s", item.id, self.after)
                self.after = item.id
                message = Message(body=str(item.data))
                await self.publisher.push(message)
        finally:
            if self.after is not None:
                await self.store.save_cursor(after=self.after)

    async def run(self) -> None:
        while True:
            items = list(await self.inventory.next_batch(after=self.after))
            if not items:
                break
            await self.push_batch(items)
