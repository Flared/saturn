from typing import Optional

from saturn_engine.core import TopicMessage
from saturn_engine.utils.log import getLogger
from saturn_engine.utils.options import OptionsSchema

from ..inventories import Inventory
from ..inventories import Item
from ..topics import Topic


class JobStore(OptionsSchema):
    async def load_cursor(self) -> Optional[str]:
        pass

    async def save_cursor(self, *, after: str) -> None:
        pass


class Job:
    def __init__(
        self, *, inventory: Inventory, publisher: Topic, store: JobStore
    ) -> None:
        self.logger = getLogger(__name__, self)
        self.inventory = inventory
        self.publisher = publisher
        self.store = store
        self.after: Optional[str] = None

    async def push_batch(self, items: list[Item]) -> None:
        try:
            for item in items:
                self.after = item.id
                message = TopicMessage(id=str(item.id), args=item.args)
                await self.publisher.publish(message, wait=True)
        finally:
            if self.after is not None:
                await self.store.save_cursor(after=self.after)

    async def run(self) -> None:
        while True:
            items = list(await self.inventory.next_batch(after=self.after))
            if not items:
                break
            await self.push_batch(items)
