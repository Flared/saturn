import dataclasses
from typing import Union

from saturn_engine.core import PipelineInfo  # noqa: F401  # Reexport for public API
from saturn_engine.core import QueuePipeline


@dataclasses.dataclass
class TopicItem:
    name: str
    type: str
    options: dict[str, object] = dataclasses.field(default_factory=dict)


@dataclasses.dataclass
class InventoryItem:
    name: str
    type: str
    options: dict[str, object] = dataclasses.field(default_factory=dict)


@dataclasses.dataclass
class ResourceItem:
    name: str
    type: str
    data: dict


@dataclasses.dataclass
class QueueItem:
    name: str
    input: Union[TopicItem, InventoryItem]
    pipeline: QueuePipeline
    output: dict[str, list[TopicItem]]


@dataclasses.dataclass
class JobDefinition:
    name: str
    template: QueueItem
    minimal_interval: str


@dataclasses.dataclass
class SyncResponse:
    items: list[QueueItem]
    resources: list[ResourceItem]
