import dataclasses

from saturn_engine.core import PipelineInfo  # noqa: F401  # Reexport for public API
from saturn_engine.core import QueuePipeline


@dataclasses.dataclass
class ResourceItem:
    id: str
    type: str
    data: dict


@dataclasses.dataclass
class QueueItem:
    id: str
    pipeline: QueuePipeline
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
    resources: list[ResourceItem]
