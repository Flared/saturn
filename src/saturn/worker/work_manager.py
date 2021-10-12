import asyncio
import dataclasses
import logging
from datetime import datetime
from datetime import timedelta
from typing import Iterator
from typing import Optional

from saturn.client.worker_manager import QueueItem
from saturn.client.worker_manager import WorkerManagerClient
from saturn.utils import flatten
from saturn.utils import tri_split

from .queues import Queue


@dataclasses.dataclass
class QueuesSync:
    add: list[Queue]
    drop: list[Queue]


ItemsQueues = dict[str, list[Queue]]


class WorkManager:
    client: WorkerManagerClient
    work_items_queues: ItemsQueues
    last_sync_at: Optional[datetime]

    def __init__(self, client: Optional[WorkerManagerClient] = None) -> None:
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.client = client or WorkerManagerClient()
        self.work_items_queues = {}
        self.last_sync_at = None
        self.sync_period = timedelta(seconds=60)

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
        add, _, drop = tri_split(sync_items_ids, current_items)

        added_items_queues: ItemsQueues = await self.build_queues_for_items(
            sync_items[i] for i in add
        )
        self.work_items_queues.update(added_items_queues)
        add_queues = list(flatten(added_items_queues.values()))

        drop_queues = list(flatten(self.work_items_queues.pop(k) for k in drop))

        return QueuesSync(add_queues, drop_queues)

    async def build_queues_for_items(self, items: Iterator[QueueItem]) -> ItemsQueues:
        tasks_items = [(item.id, item) for item in items]
        queues_tasks = [
            asyncio.create_task(self.build_queues_for_item(item))
            for _, item in tasks_items
        ]
        tasks_results = await asyncio.gather(*queues_tasks, return_exceptions=True)
        return dict(
            (i, queues)
            for (i, _), queues in zip(tasks_items, tasks_results)
            if isinstance(queues, list)
        )

    async def build_queues_for_item(self, item: QueueItem) -> list[Queue]:
        try:
            from .queues.dummy import DummyQueue

            return [DummyQueue(item.id)]
        except Exception:
            logging.exception("Failed to build queues for %s", item)
            raise
