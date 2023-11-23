import typing as t

import asyncio
import dataclasses
from contextlib import AsyncExitStack

from saturn_engine.core import Cursor
from saturn_engine.core.api import ComponentDefinition
from saturn_engine.utils.asyncutils import TasksGroup
from saturn_engine.worker.executors.scheduler import Schedulable
from saturn_engine.worker.executors.scheduler import Scheduler
from saturn_engine.worker.inventory import Inventory
from saturn_engine.worker.inventory import Item
from saturn_engine.worker.inventory import IteratorInventory
from saturn_engine.worker.services import Services

from .multi import MultiCursorsState
from .multi import MultiItems
from .multi import RootCursors
from .multi import SubCursors


class JoinInventory(IteratorInventory):
    @dataclasses.dataclass
    class Options:
        root: ComponentDefinition
        join: ComponentDefinition
        root_concurrency: int = 1
        batch_size: int = 10
        flatten: bool = True
        alias: t.Optional[str] = None

    def __init__(self, options: Options, services: Services, **kwargs: object) -> None:
        super().__init__(
            options=options,
            services=services,
            batch_size=options.batch_size,
            **kwargs,
        )
        self.options = options
        self.__services = services
        self.concurrency_sem = asyncio.BoundedSemaphore(options.root_concurrency)

        self.root_name = self.options.root.name
        self.join_name = self.options.join.name
        self.root = self.build_inventory(options.root)
        self.scheduler: Scheduler[Item] = Scheduler()

    def build_inventory(self, inventory: ComponentDefinition) -> Inventory:
        # This import must be done late since work_factory depends on this module.
        from saturn_engine.worker.work_factory import build_inventory

        return build_inventory(inventory, services=self.__services)

    async def iterate(self, after: t.Optional[Cursor] = None) -> t.AsyncIterator[Item]:
        self._multi_cursors = MultiCursorsState.from_cursor(after)

        async with (TasksGroup(name=f"join-inventory({self.root_name})") as group,):
            group.create_task(
                self.join_inventories(),
                name=f"join-inventory({self.root_name})",
            )

            async for item in self.scheduler.run():
                yield item

    async def open(self) -> None:
        await self.root.open()

    async def run(self, after: t.Optional[Cursor] = None) -> t.AsyncIterator[Item]:
        async for item in self.iterate(after=after):
            yield item

    async def join_inventories(self) -> None:
        async with (
            TasksGroup(name=f"join-subinventory({self.root_name})") as group,
            AsyncExitStack() as stack,
        ):
            stack.push_async_callback(self.scheduler.close)
            root_cursors = self._multi_cursors.process_root(self.root)

            async for item in self.root.run(after=self._multi_cursors.after):
                name = f"join-inventory.join({self.options.join.name},{item.id})"
                await self.concurrency_sem.acquire()

                subdef = dataclasses.replace(
                    self.options.join,
                    options=self.options.join.options | {"parent_item": item},
                )
                subinv = self.build_inventory(subdef)
                await subinv.open()

                subcursors = self._multi_cursors.process_sub(subinv, parent=item)

                fut = self.scheduler.add(
                    Schedulable(
                        name=name,
                        iterable=self.run_subinv(
                            parent=item, subinv=subinv, cursors=subcursors
                        ),
                    )
                )
                group.create_task(
                    self.process_item(
                        item, schedule_future=fut, cursors=root_cursors, subinv=subinv
                    )
                )
            await group.wait_all()

    async def run_subinv(
        self, *, parent: Item, subinv: Inventory, cursors: SubCursors
    ) -> t.AsyncGenerator[Item, None]:
        multi = MultiItems.from_one(parent, name=self.root_name)
        async for item in subinv.run(after=cursors.cursor):
            async with AsyncExitStack() as stack:
                stack.enter_context(cursors.process())
                await stack.enter_async_context(item.context)
                yield multi.with_item(item, name=self.join_name).as_item(
                    context=stack.pop_all(),
                    flatten=self.options.flatten,
                    alias=self.options.alias,
                )

    async def process_item(
        self,
        item: Item,
        *,
        schedule_future: asyncio.Future[None],
        cursors: RootCursors,
        subinv: Inventory,
    ) -> None:
        try:
            with cursors.process(item):
                async with item:
                    await schedule_future
                    await subinv.join()
        finally:
            self.concurrency_sem.release()

    @property
    def cursor(self) -> Cursor:
        return self._multi_cursors.as_cursor()
