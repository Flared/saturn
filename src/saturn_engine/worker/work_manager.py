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

from saturn_engine.client.worker_manager import WorkerManagerClient
from saturn_engine.core.api import LockResponse
from saturn_engine.core.api import QueueItem
from saturn_engine.core.api import ResourceItem
from saturn_engine.utils import flatten
from saturn_engine.utils.log import getLogger
from saturn_engine.worker.resources_manager import ResourceData

from . import work_factory
from .context import Context
from .work import SchedulableQueue
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
    queues: ItemsSync[SchedulableQueue]
    tasks: ItemsSync[Task]
    resources: ItemsSync[ResourceData]

    @classmethod
    def from_sync(
        cls: Type["WorkSync"],
        items_added: Iterable[WorkItems],
        items_dropped: Iterable[WorkItems],
        resources_sync: ItemsSync[ResourceData],
    ) -> "WorkSync":
        queues_added: list[SchedulableQueue] = []
        tasks_added: list[Task] = []
        queues_drop: list[SchedulableQueue] = []
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
            resources=resources_sync,
        )

    @classmethod
    def empty(cls: Type["WorkSync"]) -> "WorkSync":
        return cls(
            queues=ItemsSync.empty(),
            tasks=ItemsSync.empty(),
            resources=ItemsSync.empty(),
        )


WorkerItemsWork = dict[str, WorkItems]


class WorkManager:
    client: WorkerManagerClient
    worker_items_work: WorkerItemsWork
    worker_resources: dict[str, ResourceData]
    last_sync_at: Optional[datetime]

    def __init__(
        self, *, context: Context, client: Optional[WorkerManagerClient] = None
    ) -> None:
        self.logger = getLogger(__name__, self)
        self.client = client or WorkerManagerClient(
            http_client=context.services.http_client.session,
            base_url=context.services.config.worker_manager.url,
        )
        self.worker_items_work = {}
        self.worker_resources = {}
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
        lock_response = await self.client.lock()
        self.last_sync_at = datetime.now()

        work_items = await self.parse_work_items(lock_response)
        resources_sync = await self.parse_resources(lock_response)

        return WorkSync.from_sync(
            items_added=work_items.add,
            items_dropped=work_items.drop,
            resources_sync=resources_sync,
        )

    async def parse_work_items(
        self, lock_response: LockResponse
    ) -> ItemsSync[WorkItems]:
        current_items = set(self.worker_items_work.keys())
        sync_items = {item.name: item for item in lock_response.items}
        sync_items_ids = set(sync_items.keys())
        add = sync_items_ids - current_items
        drop = current_items - sync_items_ids

        added_items = await self.build_work_for_worker_items(sync_items[i] for i in add)
        self.worker_items_work.update(added_items)
        add_items = added_items.values()
        drop_items = [self.worker_items_work.pop(k) for k in drop]

        return ItemsSync(add=list(add_items), drop=drop_items)

    def work_items_by_name(self, name: str) -> WorkItems:
        return self.worker_items_work.get(name, WorkItems.empty())

    async def build_work_for_worker_items(
        self, items: Iterator[QueueItem]
    ) -> WorkerItemsWork:
        return {item.name: self.build_work_for_worker_item(item) for item in items}

    def build_work_for_worker_item(self, item: QueueItem) -> WorkItems:
        try:
            return work_factory.build(item, context=self.context)
        except Exception:
            self.logger.exception("Failed to build item for %s", item)
            return WorkItems.empty()

    async def parse_resources(
        self, lock_response: LockResponse
    ) -> ItemsSync[ResourceData]:
        current_items = set(self.worker_resources.keys())
        sync_items = {item.name: item for item in lock_response.resources}
        sync_items_ids = set(sync_items.keys())
        add = sync_items_ids - current_items
        drop = current_items - sync_items_ids

        added_items = {i: self.build_resource_data(sync_items[i]) for i in add}
        self.worker_resources.update(added_items)
        add_items = list(added_items.values())
        drop_items = [self.worker_resources.pop(k) for k in drop]

        return ItemsSync(add=add_items, drop=drop_items)

    def build_resource_data(self, item: ResourceItem) -> ResourceData:
        return ResourceData(
            name=item.name,
            type=item.type,
            data=item.data,
            default_delay=item.default_delay,
        )
