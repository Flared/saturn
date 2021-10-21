import dataclasses
from typing import Optional

from saturn_engine.utils.log import getLogger
from saturn_engine.utils.options import OptionsSchema

from ..inventories import Inventory
from ..queues import Publisher


class JobStore(OptionsSchema):
    async def load_cursor(self) -> Optional[str]:
        pass

    async def save_cursor(self, *, after: str) -> None:
        pass


@dataclasses.dataclass
class JobItem:
    id: str
    data: object


class Job:
    def __init__(
        self, *, inventory: Inventory, publisher: Publisher, store: JobStore
    ) -> None:
        self.logger = getLogger(__name__, self)
        self.inventory = inventory
        self.publisher = publisher
        self.store = store
        self.after: Optional[str] = None

    async def push_batch(self, items: list) -> None:
        try:
            for item in items:
                if self.after is not None and item.id <= self.after:
                    self.logger.error("Unordered items: %s <= %s", item.id, self.after)
                self.after = item.id
                await self.publisher.push(item)
        finally:
            if self.after is not None:
                await self.store.save_cursor(after=self.after)

    async def run(self, job: "Job") -> None:
        while True:
            items = list(await self.inventory.next_batch(after=job.after))
            if not items:
                break
            await job.push_batch(items)
