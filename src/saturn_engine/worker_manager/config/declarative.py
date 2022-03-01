from typing import DefaultDict

from collections import defaultdict

from saturn_engine.utils.declarative_config import UncompiledObject
from saturn_engine.utils.declarative_config import (
    load_uncompiled_objects_from_directory,
)
from saturn_engine.utils.declarative_config import load_uncompiled_objects_from_str
from saturn_engine.utils.options import fromdict

from .declarative_inventory import Inventory
from .declarative_job import Job
from .declarative_job_definition import JobDefinition
from .declarative_resource import Resource
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
        definitions.job_definitions[
            job_definition.metadata.name
        ] = job_definition.to_core_object(definitions)

    for uncompiled_job in objects_by_kind.pop(
        "SaturnJob",
        dict(),
    ).values():
        job_data: Job = fromdict(uncompiled_job.data, Job)
        definitions.jobs[job_data.metadata.name] = job_data.to_core_object(definitions)

    for uncompiled_resource in objects_by_kind.pop(
        "SaturnResource",
        dict(),
    ).values():
        resource: Resource = fromdict(uncompiled_resource.data, Resource)
        resource_item = resource.to_core_object()
        definitions.resources[resource.metadata.name] = resource_item
        definitions.resources_by_type[resource_item.type].append(resource_item)

    for object_kind in objects_by_kind.keys():
        raise Exception(f"Unsupported kind {object_kind}")

    return definitions


def load_definitions_from_str(definitions: str) -> StaticDefinitions:
    return compile_static_definitions(load_uncompiled_objects_from_str(definitions))


def load_definitions_from_directory(config_dir: str) -> StaticDefinitions:
    return compile_static_definitions(
        load_uncompiled_objects_from_directory(config_dir)
    )
