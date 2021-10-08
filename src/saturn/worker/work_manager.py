import asyncio
import dataclasses
import logging
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

    def __init__(self, client: Optional[WorkerManagerClient] = None) -> None:
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.client = client or WorkerManagerClient()
        self.work_items_queues = {}

    async def sync_queues(self) -> QueuesSync:
        sync_response = await self.client.sync()

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

            return [DummyQueue()]
        except Exception:
            logging.exception("Failed to build queues for %s", item)
            raise
