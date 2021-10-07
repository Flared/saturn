import dataclasses


@dataclasses.dataclass
class QueueItem:
    id: str
    pipeline: str
    ressources: list[str]


@dataclasses.dataclass
class JobItem(QueueItem):
    inventory: str


@dataclasses.dataclass
class WorkItem:
    job: JobItem
    queue: QueueItem


@dataclasses.dataclass
class SyncResponse:
    items: list[WorkItem]


class WorkerManagerClient:
    def __init__(self) -> None:
        pass

    def sync(self) -> SyncResponse:
        return SyncResponse(items=[])
