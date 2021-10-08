import dataclasses
from typing import Optional

from saturn.client.worker_manager import WorkerManagerClient
from saturn.utils import flatten
from saturn.utils import tri_split

from .queue import Queue


@dataclasses.dataclass
class QueuesSync:
    add: list[Queue]
    drop: list[Queue]


ItemsQueues = dict[str, list[Queue]]


class WorkManager:
    client: WorkerManagerClient
    work_items_queues: ItemsQueues

    def __init__(self, client: Optional[WorkerManagerClient] = None) -> None:
        self.client = client or WorkerManagerClient()
        self.work_items_queues = {}

    async def sync_queues(self) -> QueuesSync:
        sync_response = await self.client.sync()

        current_items = set(self.work_items_queues.keys())
        sync_items = {item.id for item in sync_response.items}
        add, _, drop = tri_split(sync_items, current_items)

        added_items_queues: ItemsQueues = {i: [Queue()] for i in add}
        self.work_items_queues.update(added_items_queues)
        add_queues = list(flatten(added_items_queues.values()))

        drop_queues = list(flatten(self.work_items_queues.pop(k) for k in drop))

        return QueuesSync(add_queues, drop_queues)
