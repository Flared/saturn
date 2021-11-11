import dataclasses
from collections import defaultdict

from saturn_engine.core.api import InventoryItem
from saturn_engine.core.api import JobDefinition
from saturn_engine.core.api import ResourceItem
from saturn_engine.core.api import TopicItem


@dataclasses.dataclass
class StaticDefinitions:
    inventories: dict[str, InventoryItem] = dataclasses.field(default_factory=dict)
    topics: dict[str, TopicItem] = dataclasses.field(default_factory=dict)
    job_definitions: dict[str, JobDefinition] = dataclasses.field(default_factory=dict)
    resources: dict[str, ResourceItem] = dataclasses.field(default_factory=dict)
    resources_by_type: dict[str, list[ResourceItem]] = dataclasses.field(
        default_factory=lambda: defaultdict(list)
    )
