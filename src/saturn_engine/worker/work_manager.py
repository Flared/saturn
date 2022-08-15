from typing import Generic
from typing import Iterator
from typing import Optional
from typing import Type
from typing import TypeVar

import asyncio
import dataclasses
from datetime import datetime
from datetime import timedelta

from saturn_engine.client.worker_manager import WorkerManagerClient
from saturn_engine.core.api import Executor
from saturn_engine.core.api import LockResponse
from saturn_engine.core.api import QueueItem
from saturn_engine.core.api import ResourceItem
from saturn_engine.utils.log import getLogger
from saturn_engine.worker import work_factory
from saturn_engine.worker.executors.executable import ExecutableQueue
from saturn_engine.worker.resources.manager import ResourceData
from saturn_engine.worker.services import Services
from saturn_engine.worker.services.http_client import HttpClient

T = TypeVar("T")


@dataclasses.dataclass
class ItemsSync(Generic[T]):
    add: list[T]
    drop: list[T]

    @classmethod
    def empty(cls: Type["ItemsSync[T]"]) -> "ItemsSync[T]":
        return cls(add=[], drop=[])


@dataclasses.dataclass
class WorkSync:
    queues: ItemsSync[ExecutableQueue]
    resources: ItemsSync[ResourceData]
    executors: ItemsSync[Executor]

    @classmethod
    def empty(cls: Type["WorkSync"]) -> "WorkSync":
        return cls(
            queues=ItemsSync.empty(),
            resources=ItemsSync.empty(),
            executors=ItemsSync.empty(),
        )


WorkerItems = dict[str, ExecutableQueue]


class WorkManager:
    def __init__(
        self, *, services: Services, client: Optional[WorkerManagerClient] = None
    ) -> None:
        self.logger = getLogger(__name__, self)
        http_client = services.cast_service(HttpClient)
        self.client: WorkerManagerClient = client or WorkerManagerClient(
            http_client=http_client.session,
            base_url=services.s.config.c.worker_manager_url,
        )
        self.worker_items: WorkerItems = {}
        self.worker_resources: dict[str, ResourceData] = {}
        self.worker_executors: dict[str, Executor] = {}
        self.last_sync_at: Optional[datetime] = None
        self.sync_period = timedelta(seconds=60)
        self.services = services

    async def sync(self) -> WorkSync:
        if self.last_sync_at:
            last_sync_elapsed = datetime.now() - self.last_sync_at
            if last_sync_elapsed < self.sync_period:
                await asyncio.sleep(
                    (self.sync_period - last_sync_elapsed).total_seconds()
                )
        lock_response = await self.client.lock()
        self.last_sync_at = datetime.now()

        queues_sync = await self.load_queues(lock_response)
        resources_sync = await self.load_resources(lock_response)
        executors_sync = await self.load_executors(lock_response)

        return WorkSync(
            queues=queues_sync,
            resources=resources_sync,
            executors=executors_sync,
        )

    async def load_queues(
        self, lock_response: LockResponse
    ) -> ItemsSync[ExecutableQueue]:
        current_items = set(self.worker_items.keys())
        sync_items = {item.name: item for item in lock_response.items}
        sync_items_ids = set(sync_items.keys())
        add = sync_items_ids - current_items
        drop = current_items - sync_items_ids

        added_items = await self.build_queues_for_worker_items(
            sync_items[i] for i in add
        )
        self.worker_items.update(added_items)
        add_items = list(added_items.values())
        drop_items = [self.worker_items.pop(k) for k in drop]

        return ItemsSync(add=add_items, drop=drop_items)

    def work_queue_by_name(self, name: str) -> Optional[ExecutableQueue]:
        return self.worker_items.get(name)

    async def build_queues_for_worker_items(
        self, items: Iterator[QueueItem]
    ) -> WorkerItems:
        return {
            item.name: queue
            for item in items
            if (queue := await self.build_queue_for_worker_item(item))
        }

    async def build_queue_for_worker_item(
        self, item: QueueItem
    ) -> Optional[ExecutableQueue]:
        try:

            @self.services.s.hooks.work_queue_built.emit
            async def scope(item: QueueItem) -> ExecutableQueue:
                return work_factory.build(item, services=self.services)

            return await scope(item)
        except Exception:
            return None

    async def load_resources(
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

    async def load_executors(self, lock_response: LockResponse) -> ItemsSync[Executor]:
        current_items = set(self.worker_executors.keys())
        sync_items = {item.name: item for item in lock_response.executors}
        sync_items_ids = set(sync_items.keys())
        add = sync_items_ids - current_items
        drop = current_items - sync_items_ids

        added_items = {i: sync_items[i] for i in add}
        self.worker_executors.update(added_items)
        add_items = list(added_items.values())
        drop_items = [self.worker_executors.pop(k) for k in drop]

        return ItemsSync(add=add_items, drop=drop_items)

    def build_resource_data(self, item: ResourceItem) -> ResourceData:
        return ResourceData(
            name=item.name,
            type=item.type,
            data=item.data,
            default_delay=item.default_delay,
        )
