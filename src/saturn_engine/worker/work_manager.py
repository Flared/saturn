import asyncio
import dataclasses
from collections.abc import Iterable
from datetime import datetime
from datetime import timedelta
from typing import Generic
from typing import Iterator
from typing import Optional
from typing import Type
from typing import TypeVar

from saturn_engine.client.worker_manager import QueueItem
from saturn_engine.client.worker_manager import WorkerManagerClient
from saturn_engine.utils import flatten
from saturn_engine.utils.log import getLogger

from . import work_factory
from .context import Context
from .queues import Queue
from .work import WorkItems

T = TypeVar("T")
Task = asyncio.Task


@dataclasses.dataclass
class ItemsSync(Generic[T]):
    add: list[T]
    drop: list[T]

    @classmethod
    def empty(cls: Type["ItemsSync[T]"]) -> "ItemsSync[T]":
        return cls(add=[], drop=[])


@dataclasses.dataclass
class WorkSync:
    queues: ItemsSync[Queue]
    tasks: ItemsSync[Task]

    @classmethod
    def from_work_items(
        cls: Type["WorkSync"],
        items_added: Iterable[WorkItems],
        items_dropped: Iterable[WorkItems],
    ) -> "WorkSync":
        queues_added: list[Queue] = []
        tasks_added: list[Task] = []
        queues_drop: list[Queue] = []
        tasks_drop: list[Task] = []
        if items_added:
            queues_added, tasks_added = [
                list(flatten(x))
                for x in zip(*[(i.queues, i.tasks) for i in items_added])
            ]

        if items_dropped:
            queues_drop, tasks_drop = [
                list(flatten(x))
                for x in zip(*[(i.queues, i.tasks) for i in items_dropped])
            ]

        return cls(
            queues=ItemsSync(add=queues_added, drop=queues_drop),
            tasks=ItemsSync(add=tasks_added, drop=tasks_drop),
        )

    @classmethod
    def empty(cls: Type["WorkSync"]) -> "WorkSync":
        return cls(
            queues=ItemsSync.empty(),
            tasks=ItemsSync.empty(),
        )


WorkerItemsWork = dict[str, WorkItems]


class WorkManager:
    client: WorkerManagerClient
    worker_items_work: WorkerItemsWork
    last_sync_at: Optional[datetime]

    def __init__(
        self, *, context: Context, client: Optional[WorkerManagerClient] = None
    ) -> None:
        self.logger = getLogger(__name__, self)
        self.client = client or WorkerManagerClient()
        self.worker_items_work = {}
        self.last_sync_at = None
        self.sync_period = timedelta(seconds=60)
        self.context = context

    async def sync(self) -> WorkSync:
        if self.last_sync_at:
            last_sync_elapsed = datetime.now() - self.last_sync_at
            if last_sync_elapsed < self.sync_period:
                await asyncio.sleep(
                    (self.sync_period - last_sync_elapsed).total_seconds()
                )
        sync_response = await self.client.sync()
        self.last_sync_at = datetime.now()

        current_items = set(self.worker_items_work.keys())
        sync_items = {item.id: item for item in sync_response.items}
        sync_items_ids = set(sync_items.keys())
        add = sync_items_ids - current_items
        drop = current_items - sync_items_ids

        added_items = await self.build_work_for_worker_items(sync_items[i] for i in add)
        self.worker_items_work.update(added_items)
        add_items = added_items.values()
        drop_items = [self.worker_items_work.pop(k) for k in drop]

        return WorkSync.from_work_items(add_items, drop_items)

    def work_items_by_id(self, id: str) -> WorkItems:
        return self.worker_items_work.get(id, WorkItems.empty())

    async def build_work_for_worker_items(
        self, items: Iterator[QueueItem]
    ) -> WorkerItemsWork:
        return {item.id: self.build_work_for_worker_item(item) for item in items}

    def build_work_for_worker_item(self, item: QueueItem) -> WorkItems:
        try:
            return work_factory.build(item, context=self.context)
        except Exception:
            self.logger.exception("Failed to build queues for %s", item)
            return WorkItems.empty()
