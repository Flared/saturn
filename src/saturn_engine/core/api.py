import dataclasses
from typing import Any
from typing import Generic
from typing import TypeVar
from typing import Union

from saturn_engine.core import PipelineInfo  # noqa: F401  # Reexport for public API
from saturn_engine.core import QueuePipeline
from saturn_engine.utils.options import ObjectUnion
from saturn_engine.utils.options import field

T = TypeVar("T")


@dataclasses.dataclass
class TopicItem:
    name: str
    type: str
    options: dict[str, Any] = dataclasses.field(default_factory=dict)


@dataclasses.dataclass
class InventoryItem:
    name: str
    type: str
    options: dict[str, Any] = dataclasses.field(default_factory=dict)


@dataclasses.dataclass
class ResourceItem:
    name: str
    type: str
    data: dict[str, Any]


@dataclasses.dataclass
class QueueItem:
    name: str
    pipeline: QueuePipeline
    output: dict[str, list[TopicItem]]
    input: Union[TopicItem, InventoryItem] = field(
        ObjectUnion(union={"topic": TopicItem, "inventory": InventoryItem})
    )


@dataclasses.dataclass
class JobDefinition:
    name: str
    template: QueueItem
    minimal_interval: str


@dataclasses.dataclass
class LockResponse:
    items: list[QueueItem]
    resources: list[ResourceItem]


@dataclasses.dataclass
class LockInput:
    worker_id: str


@dataclasses.dataclass
class ListResponse(Generic[T]):
    items: list[T]


@dataclasses.dataclass
class JobDefinitionsResponse(ListResponse[JobDefinition]):
    items: list[JobDefinition]


@dataclasses.dataclass
class InventoriesResponse(ListResponse[InventoryItem]):
    items: list[InventoryItem]
