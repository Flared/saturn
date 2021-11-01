import dataclasses


@dataclasses.dataclass
class QueueItem:
    id: str
    pipeline: str
    ressources: list[str]
    options: dict


@dataclasses.dataclass
class Inventory:
    name: str
    type: str
    options: dict


@dataclasses.dataclass
class JobItem(QueueItem):
    inventory: Inventory


@dataclasses.dataclass
class DummyItem(QueueItem):
    pass


@dataclasses.dataclass
class DummyJob(JobItem):
    pass


@dataclasses.dataclass
class MemoryItem(QueueItem):
    pass


@dataclasses.dataclass
class SyncResponse:
    items: list[QueueItem]
