from typing import AsyncIterator
from typing import Optional

import dataclasses

import asyncstdlib as alib

from saturn_engine.core.api import ComponentDefinition
from saturn_engine.worker.services import Services

from . import Inventory
from . import Item


class BatchingInventory(Inventory):
    @dataclasses.dataclass
    class Options:
        inventory: ComponentDefinition
        batch_size: int = 10

    def __init__(
        self, *, options: Options, services: Services, **kwargs: object
    ) -> None:
        self.batch_size = options.batch_size

        # This import must be done late since work_factory depends on this module.
        from saturn_engine.worker.work_factory import build_inventory

        self.inventory = build_inventory(options.inventory, services=services)

    async def next_batch(self, after: Optional[str] = None) -> list[Item]:
        batch: list[Item] = await alib.list(
            alib.islice(self.inventory.iterate(after=after), self.batch_size)
        )
        return batch

    async def iterate(self, after: Optional[str] = None) -> AsyncIterator[Item]:
        while True:
            batch = await self.next_batch(after)
            if not batch:
                return

            last_item = batch[-1]
            yield Item(
                id=last_item.id,
                cursor=last_item.cursor,
                args={"batch": [item.args for item in batch]},
            )
            after = last_item.cursor
