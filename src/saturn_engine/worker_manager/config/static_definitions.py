import typing as t

import dataclasses
from collections import defaultdict

from saturn_engine.core.api import ComponentDefinition
from saturn_engine.core.api import JobDefinition
from saturn_engine.core.api import QueueItem
from saturn_engine.core.api import ResourceItem
from saturn_engine.core.api import ResourcesProviderItem


@dataclasses.dataclass
class StaticDefinitions:
    executors: dict[str, ComponentDefinition] = dataclasses.field(default_factory=dict)
    inventories: dict[str, ComponentDefinition] = dataclasses.field(
        default_factory=dict
    )
    topics: dict[str, ComponentDefinition] = dataclasses.field(default_factory=dict)
    job_definitions: dict[str, JobDefinition] = dataclasses.field(default_factory=dict)
    jobs: dict[str, QueueItem] = dataclasses.field(default_factory=dict)
    resources_providers: dict[str, ResourcesProviderItem] = dataclasses.field(
        default_factory=dict
    )
    resources: dict[str, ResourceItem] = dataclasses.field(default_factory=dict)
    resources_by_type: dict[
        str, list[t.Union[ResourceItem, ResourcesProviderItem]]
    ] = dataclasses.field(default_factory=lambda: defaultdict(list))
