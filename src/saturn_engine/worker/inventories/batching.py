import typing as t

import dataclasses
from contextlib import AsyncExitStack

import asyncstdlib as alib

from saturn_engine.core import Cursor
from saturn_engine.core.api import ComponentDefinition
from saturn_engine.worker.inventory import Item
from saturn_engine.worker.inventory import IteratorInventory
from saturn_engine.worker.services import Services


class BatchingInventory(IteratorInventory):
    @dataclasses.dataclass
    class Options:
        inventory: ComponentDefinition
        batch_size: int = 10

    def __init__(
        self, *, options: Options, services: Services, **kwargs: object
    ) -> None:
        super().__init__(batch_size=options.batch_size)

        # This import must be done late since work_factory depends on this module.
        from saturn_engine.worker.work_factory import build_inventory

        self.inventory = build_inventory(options.inventory, services=services)

    async def _next_batch(self) -> list[Item]:
        batch: list[Item] = await alib.list(
            alib.islice(self._run_iter, self.batch_size)
        )
        return batch

    async def iterate(self, after: t.Optional[Cursor] = None) -> t.AsyncIterator[Item]:
        async with alib.scoped_iter(self.inventory.run(after=after)) as run_iter:
            self._run_iter = run_iter

            while True:
                batch = await self._next_batch()
                if not batch:
                    return

                last_item = batch[-1]
                async with AsyncExitStack() as stack:
                    for item in batch:
                        await stack.enter_async_context(item)
                    yield dataclasses.replace(
                        last_item,
                        args={"batch": [item.args for item in batch]},
                        context=stack.pop_all(),
                    )

    @property
    def cursor(self) -> t.Optional[Cursor]:
        return self.inventory.cursor
