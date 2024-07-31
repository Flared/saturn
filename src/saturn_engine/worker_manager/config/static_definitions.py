import typing as t

import dataclasses
from collections import defaultdict

from saturn_engine.core.api import ComponentDefinition
from saturn_engine.core.api import JobDefinition
from saturn_engine.core.api import QueueItem
from saturn_engine.core.api import ResourceItem
from saturn_engine.core.api import ResourcesProviderItem
from saturn_engine.utils.declarative_config import BaseObject


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
    resources_by_type: dict[str, list[t.Union[ResourceItem, ResourcesProviderItem]]] = (
        dataclasses.field(default_factory=lambda: defaultdict(list))
    )

    def add(self, obj: BaseObject) -> None:
        from saturn_engine.worker_manager.config.declarative_dynamic_topology import (
            DynamicTopology,
        )
        from saturn_engine.worker_manager.config.declarative_executor import Executor
        from saturn_engine.worker_manager.config.declarative_inventory import Inventory
        from saturn_engine.worker_manager.config.declarative_job import Job
        from saturn_engine.worker_manager.config.declarative_job_definition import (
            JobDefinition as DeclarativeJobDefinition,
        )
        from saturn_engine.worker_manager.config.declarative_resource import Resource
        from saturn_engine.worker_manager.config.declarative_resource import (
            ResourcesProvider,
        )
        from saturn_engine.worker_manager.config.declarative_topic_item import TopicItem

        if isinstance(obj, Executor):
            self.executors[obj.metadata.name] = obj.to_core_object()
        elif isinstance(obj, Inventory):
            self.inventories[obj.metadata.name] = obj.to_core_object()
        elif isinstance(obj, TopicItem):
            self.topics[obj.metadata.name] = obj.to_core_object()
        elif isinstance(obj, DeclarativeJobDefinition):
            for core_job_definition in obj.to_core_objects(self):
                self.job_definitions[core_job_definition.name] = core_job_definition
        elif isinstance(obj, Job):
            for queue_item in obj.to_core_objects(self):
                self.jobs[queue_item.name] = queue_item
        elif isinstance(obj, ResourcesProvider):
            resources_provider_item = obj.to_core_object()
            self.resources_providers[resources_provider_item.name] = (
                resources_provider_item
            )
            self.resources_by_type[resources_provider_item.resource_type].append(
                resources_provider_item
            )
        elif isinstance(obj, Resource):
            # If we have concurrency for our resource then
            # we append an index at the end in order
            # to differentiate the instances of said resource
            for i in range(1, obj.spec.concurrency + 1):
                resource_item = obj.to_core_object()
                if obj.spec.concurrency > 1:
                    resource_item.name = f"{obj.metadata.name}-{i}"
                self.resources[resource_item.name] = resource_item
                self.resources_by_type[resource_item.type].append(resource_item)
        elif isinstance(obj, DynamicTopology):
            obj.update_static_definitions(self)
        else:
            raise Exception(f"Unsupported base object type: {type(obj)}")
