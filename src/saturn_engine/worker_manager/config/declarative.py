from typing import DefaultDict

import dataclasses
import re
from collections import defaultdict

from saturn_engine.utils.declarative_config import UncompiledObject
from saturn_engine.utils.declarative_config import load_uncompiled_objects_from_path
from saturn_engine.utils.declarative_config import load_uncompiled_objects_from_str
from saturn_engine.utils.options import fromdict

from .declarative_executor import Executor
from .declarative_inventory import Inventory
from .declarative_job import Job
from .declarative_job_definition import JobDefinition
from .declarative_resource import Resource
from .declarative_resource import ResourcesProvider
from .declarative_topic_item import TopicItem
from .static_definitions import StaticDefinitions


def compile_static_definitions(
    uncompiled_objects: list[UncompiledObject],
) -> StaticDefinitions:
    objects_by_kind: DefaultDict[str, dict[str, UncompiledObject]] = defaultdict(dict)
    for uncompiled_object in uncompiled_objects:
        if uncompiled_object.name in objects_by_kind[uncompiled_object.kind]:
            raise Exception(
                f"{uncompiled_object.kind}/{uncompiled_object.name} already exists"
            )
        objects_by_kind[uncompiled_object.kind][
            uncompiled_object.name
        ] = uncompiled_object

    definitions: StaticDefinitions = StaticDefinitions()

    for uncompiled_executor in objects_by_kind.pop(
        "SaturnExecutor",
        dict(),
    ).values():
        executor: Executor = fromdict(uncompiled_executor.data, Executor)
        definitions.executors[executor.metadata.name] = executor.to_core_object()

    for uncompiled_inventory in objects_by_kind.pop(
        "SaturnInventory",
        dict(),
    ).values():
        inventory: Inventory = fromdict(uncompiled_inventory.data, Inventory)
        definitions.inventories[inventory.metadata.name] = inventory.to_core_object()

    for uncompiled_topic in objects_by_kind.pop(
        "SaturnTopic",
        dict(),
    ).values():
        topic_item: TopicItem = fromdict(uncompiled_topic.data, TopicItem)
        definitions.topics[topic_item.metadata.name] = topic_item.to_core_object()

    for uncompiled_job_definition in objects_by_kind.pop(
        "SaturnJobDefinition",
        dict(),
    ).values():
        job_definition: JobDefinition = fromdict(
            uncompiled_job_definition.data, JobDefinition
        )
        for core_job_definition in job_definition.to_core_objects(definitions):
            definitions.job_definitions[core_job_definition.name] = core_job_definition

    for uncompiled_job in objects_by_kind.pop(
        "SaturnJob",
        dict(),
    ).values():
        job_data: Job = fromdict(uncompiled_job.data, Job)
        for queue_item in job_data.to_core_objects(definitions):
            definitions.jobs[queue_item.name] = queue_item

    for uncompiled_resource in objects_by_kind.pop(
        "SaturnResource",
        dict(),
    ).values():
        resource: Resource = fromdict(uncompiled_resource.data, Resource)
        # If we have concurrency for our resource then
        # we append an index at the end in order
        # to differentiate the instances of said resource
        for i in range(1, resource.spec.concurrency + 1):
            resource_item = resource.to_core_object()
            if resource.spec.concurrency > 1:
                resource_item.name = f"{resource.metadata.name}-{i}"
            definitions.resources[resource_item.name] = resource_item
            definitions.resources_by_type[resource_item.type].append(resource_item)

    for uncompied_resources_provider in objects_by_kind.pop(
        "SaturnResourcesProvider",
        dict(),
    ).values():
        resources_provider = fromdict(
            uncompied_resources_provider.data, ResourcesProvider
        )
        resources_provider_item = resources_provider.to_core_object()
        definitions.resources_providers[
            resources_provider_item.name
        ] = resources_provider_item
        definitions.resources_by_type[resources_provider_item.resource_type].append(
            resources_provider_item
        )

    for object_kind in objects_by_kind.keys():
        raise Exception(f"Unsupported kind {object_kind}")

    return definitions


def load_definitions_from_str(definitions: str) -> StaticDefinitions:
    return compile_static_definitions(load_uncompiled_objects_from_str(definitions))


def load_definitions_from_path(config_dir: str) -> StaticDefinitions:
    return compile_static_definitions(load_uncompiled_objects_from_path(config_dir))


def filter_with_jobs_selector(
    *, selector: str, definitions: StaticDefinitions
) -> StaticDefinitions:
    pattern = re.compile(selector)
    jobs = {name: job for name, job in definitions.jobs.items() if pattern.search(name)}
    job_definitions = {
        name: job
        for name, job in definitions.job_definitions.items()
        if pattern.search(name)
    }
    return dataclasses.replace(definitions, jobs=jobs, job_definitions=job_definitions)
