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
class SyncResponse:
    items: list[QueueItem]


class WorkerManagerClient:
    def __init__(self) -> None:
        pass

    async def sync(self) -> SyncResponse:
        return SyncResponse(items=[])
