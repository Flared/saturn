from typing import Any
from typing import Generic
from typing import Optional
from typing import TypeVar
from typing import Union

import dataclasses
from datetime import datetime

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
    default_delay: float = 0


@dataclasses.dataclass
class QueueItem:
    name: str
    pipeline: QueuePipeline
    output: dict[str, list[TopicItem]]
    input: Union[TopicItem, InventoryItem] = field(
        ObjectUnion(union={"topic": TopicItem, "inventory": InventoryItem})
    )
    executor: str = "default"


@dataclasses.dataclass
class JobDefinition:
    name: str
    template: QueueItem
    minimal_interval: str


@dataclasses.dataclass
class Executor:
    name: str
    type: str
    options: dict[str, Any] = dataclasses.field(default_factory=dict)


@dataclasses.dataclass
class LockResponse:
    items: list[QueueItem]
    resources: list[ResourceItem]
    executors: list[Executor]


@dataclasses.dataclass
class LockInput:
    worker_id: str


@dataclasses.dataclass
class ListResponse(Generic[T]):
    items: list[T]


@dataclasses.dataclass
class ItemResponse(Generic[T]):
    data: T


@dataclasses.dataclass
class JobDefinitionsResponse(ListResponse[JobDefinition]):
    items: list[JobDefinition]


@dataclasses.dataclass
class TopicsResponse(ListResponse[TopicItem]):
    items: list[TopicItem]


@dataclasses.dataclass
class InventoriesResponse(ListResponse[InventoryItem]):
    items: list[InventoryItem]


@dataclasses.dataclass
class JobItem:
    name: str
    completed_at: Optional[datetime]
    started_at: datetime
    cursor: Optional[str]
    error: Optional[str]


@dataclasses.dataclass
class JobInput:
    cursor: Optional[str] = None
    completed_at: Optional[datetime] = None
    error: Optional[str] = None


@dataclasses.dataclass
class JobsResponse(ListResponse[JobItem]):
    items: list[JobItem]


@dataclasses.dataclass
class JobResponse(ItemResponse[JobItem]):
    data: JobItem


@dataclasses.dataclass
class UpdateResponse:
    pass


@dataclasses.dataclass
class JobsSyncResponse:
    pass
