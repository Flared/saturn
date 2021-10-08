import dataclasses
from typing import Optional

from saturn.client.worker_manager import WorkerManagerClient

from .queue import Queue


@dataclasses.dataclass
class QueuesSync:
    add: list[Queue]
    keep: list[Queue]
    drop: list[Queue]


class WorkManager:
    def __init__(self, client: Optional[WorkerManagerClient] = None) -> None:
        self.client = client

    async def sync_queues(self) -> QueuesSync:
        return QueuesSync([], [], [])
