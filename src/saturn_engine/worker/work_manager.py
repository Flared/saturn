import asyncio
import dataclasses
from datetime import datetime
from datetime import timedelta
from typing import Iterator
from typing import Optional

from saturn_engine.client.worker_manager import QueueItem
from saturn_engine.client.worker_manager import WorkerManagerClient
from saturn_engine.utils import flatten
from saturn_engine.utils.log import getLogger

from .queues import Queue
from .queues import factory as queues_factory
from .queues.context import QueueContext


@dataclasses.dataclass
class QueuesSync:
    add: list[Queue]
    drop: list[Queue]


ItemsQueues = dict[str, list[Queue]]


class WorkManager:
    client: WorkerManagerClient
    work_items_queues: ItemsQueues
    last_sync_at: Optional[datetime]

    def __init__(
        self, *, context: QueueContext, client: Optional[WorkerManagerClient] = None
    ) -> None:
        self.logger = getLogger(__name__, self)
        self.client = client or WorkerManagerClient()
        self.work_items_queues = {}
        self.last_sync_at = None
        self.sync_period = timedelta(seconds=60)
        self.context = context

    async def sync_queues(self) -> QueuesSync:
        if self.last_sync_at:
            last_sync_elapsed = datetime.now() - self.last_sync_at
            if last_sync_elapsed < self.sync_period:
                await asyncio.sleep(
                    (self.sync_period - last_sync_elapsed).total_seconds()
                )
        sync_response = await self.client.sync()
        self.last_sync_at = datetime.now()

        current_items = set(self.work_items_queues.keys())
        sync_items = {item.id: item for item in sync_response.items}
        sync_items_ids = set(sync_items.keys())
        add = sync_items_ids - current_items
        drop = current_items - sync_items_ids

        added_items_queues: ItemsQueues = await self.build_queues_for_items(
            sync_items[i] for i in add
        )
        self.work_items_queues.update(added_items_queues)
        add_queues = list(flatten(added_items_queues.values()))

        drop_queues = list(flatten(self.work_items_queues.pop(k) for k in drop))

        return QueuesSync(add_queues, drop_queues)

    def queues_by_id(self, id: str) -> list[Queue]:
        return self.work_items_queues.get(id, [])

    async def build_queues_for_items(self, items: Iterator[QueueItem]) -> ItemsQueues:
        return {item.id: self.build_queues_for_item(item) for item in items}

    def build_queues_for_item(self, item: QueueItem) -> list[Queue]:
        try:
            return queues_factory.build(item, context=self.context)
        except Exception:
            self.logger.exception("Failed to build queues for %s", item)
            return []
