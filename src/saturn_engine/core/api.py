import dataclasses


@dataclasses.dataclass
class QueueItem:
    id: str
    pipeline: str
    ressources: list[str]
    options: dict


@dataclasses.dataclass
class JobItem(QueueItem):
    inventory: str


@dataclasses.dataclass
class DummyItem(QueueItem):
    pass


@dataclasses.dataclass
class SyncResponse:
    items: list[QueueItem]
