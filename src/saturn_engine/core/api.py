import typing as t
from typing import Any
from typing import Generic
from typing import Optional
from typing import TypeVar

import dataclasses
from dataclasses import field
from datetime import datetime

from saturn_engine.utils import utcnow

from .pipeline import PipelineInfo  # noqa: F401  # Reexport for public API
from .pipeline import QueuePipeline
from .types import Cursor
from .types import JobId

T = TypeVar("T")


@dataclasses.dataclass
class ComponentDefinition:
    name: str
    type: str
    options: dict[str, Any] = field(default_factory=dict)


@dataclasses.dataclass
class ResourceRateLimitItem:
    rate_limits: list[str]
    strategy: str = "fixed-window"


@dataclasses.dataclass
class ResourceItem:
    name: str
    type: str
    data: dict[str, Any]
    default_delay: float = 0
    rate_limit: Optional[ResourceRateLimitItem] = None


@dataclasses.dataclass
class ResourcesProviderItem:
    name: str
    type: str
    resource_type: str
    options: dict[str, Any] = field(default_factory=dict)


@dataclasses.dataclass
class QueueItem:
    name: JobId
    pipeline: QueuePipeline
    output: dict[str, list[ComponentDefinition]]
    input: ComponentDefinition
    config: dict[str, Any] = field(default_factory=dict)
    labels: dict[str, str] = field(default_factory=dict)
    executor: str = "default"

    def with_state(self, state: "QueueItemState") -> "QueueItemWithState":
        return QueueItemWithState(**self.__dict__, state=state)


@dataclasses.dataclass
class QueueItemState:
    cursor: t.Optional[Cursor] = None
    started_at: datetime = field(default_factory=utcnow)


@dataclasses.dataclass
class QueueItemWithState(QueueItem):
    state: QueueItemState = field(default_factory=QueueItemState)


@dataclasses.dataclass
class JobDefinition:
    name: str
    template: QueueItem
    minimal_interval: str


@dataclasses.dataclass
class LockResponse:
    items: list[QueueItemWithState]
    resources: list[ResourceItem]
    resources_providers: list[ResourcesProviderItem]
    executors: list[ComponentDefinition]


@dataclasses.dataclass
class LockInput:
    worker_id: str


@dataclasses.dataclass
class FetchCursorsStatesInput:
    cursors: dict[JobId, list[Cursor]]


@dataclasses.dataclass
class FetchCursorsStatesResponse:
    cursors: dict[JobId, dict[Cursor, t.Optional[dict]]]


@dataclasses.dataclass
class JobCompletion:
    completed_at: datetime
    error: t.Optional[str] = None


@dataclasses.dataclass
class JobState:
    cursor: t.Optional[Cursor] = None
    cursors_states: dict[Cursor, dict] = dataclasses.field(default_factory=dict)
    completion: t.Optional[JobCompletion] = None


@dataclasses.dataclass
class JobsStates:
    jobs: t.Mapping[JobId, JobState] = dataclasses.field(default_factory=dict)


@dataclasses.dataclass
class JobsStatesSyncInput:
    state: JobsStates


@dataclasses.dataclass
class JobsStatesSyncResponse:
    pass


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
class TopicsResponse(ListResponse[ComponentDefinition]):
    items: list[ComponentDefinition]


@dataclasses.dataclass
class InventoriesResponse(ListResponse[ComponentDefinition]):
    items: list[ComponentDefinition]


@dataclasses.dataclass
class JobItem:
    name: JobId
    started_at: datetime
    completed_at: Optional[datetime] = None
    cursor: Optional[Cursor] = None
    error: Optional[str] = None

    enabled: bool = True
    assigned_to: Optional[str] = None
    assigned_at: Optional[datetime] = None


@dataclasses.dataclass
class JobInput:
    cursor: Optional[Cursor] = None
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
