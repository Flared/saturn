from typing import DefaultDict

import dataclasses
import logging
import re
from collections import defaultdict

from saturn_engine.models.topology_patches import TopologyPatch
from saturn_engine.utils import dict as dict_utils
from saturn_engine.utils.declarative_config import UncompiledObject
from saturn_engine.utils.declarative_config import load_uncompiled_objects_from_path
from saturn_engine.utils.declarative_config import load_uncompiled_objects_from_str
from saturn_engine.utils.options import fromdict

from .declarative_dynamic_topology import DYNAMIC_TOPOLOGY_KIND
from .declarative_dynamic_topology import DynamicTopology
from .declarative_executor import EXECUTOR_KIND
from .declarative_executor import Executor
from .declarative_inventory import INVENTORY_KIND
from .declarative_inventory import Inventory
from .declarative_job import JOB_KIND
from .declarative_job import Job
from .declarative_job_definition import JOB_DEFINITION_KIND
from .declarative_job_definition import JobDefinition
from .declarative_resource import RESOURCE_KIND
from .declarative_resource import RESOURCE_PROVIDER_KIND
from .declarative_resource import Resource
from .declarative_resource import ResourcesProvider
from .declarative_topic_item import TOPIC_ITEM_KIND
from .declarative_topic_item import TopicItem
from .static_definitions import StaticDefinitions


def compile_static_definitions(
    uncompiled_objects: list[UncompiledObject],
    patches: list[TopologyPatch] | None = None,
) -> StaticDefinitions:
    objects_by_kind: DefaultDict[str, dict[str, UncompiledObject]] = defaultdict(dict)

    if patches:
        uncompiled_objects = merge_with_patches(
            uncompiled_objects=uncompiled_objects, patches=patches
        )

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
        EXECUTOR_KIND,
        dict(),
    ).values():
        executor: Executor = fromdict(uncompiled_executor.data, Executor)
        definitions.add(executor)

    for uncompiled_inventory in objects_by_kind.pop(
        INVENTORY_KIND,
        dict(),
    ).values():
        inventory: Inventory = fromdict(uncompiled_inventory.data, Inventory)
        definitions.add(inventory)

    for uncompiled_topic in objects_by_kind.pop(
        TOPIC_ITEM_KIND,
        dict(),
    ).values():
        topic_item: TopicItem = fromdict(uncompiled_topic.data, TopicItem)
        definitions.add(topic_item)

    for uncompiled_job_definition in objects_by_kind.pop(
        JOB_DEFINITION_KIND,
        dict(),
    ).values():
        job_definition: JobDefinition = fromdict(
            uncompiled_job_definition.data, JobDefinition
        )
        definitions.add(job_definition)

    for uncompiled_job in objects_by_kind.pop(
        JOB_KIND,
        dict(),
    ).values():
        job_data: Job = fromdict(uncompiled_job.data, Job)
        definitions.add(job_data)

    for uncompiled_resource in objects_by_kind.pop(
        RESOURCE_KIND,
        dict(),
    ).values():
        resource: Resource = fromdict(uncompiled_resource.data, Resource)
        definitions.add(resource)

    for uncompied_resources_provider in objects_by_kind.pop(
        RESOURCE_PROVIDER_KIND,
        dict(),
    ).values():
        resources_provider = fromdict(
            uncompied_resources_provider.data, ResourcesProvider
        )
        definitions.add(resources_provider)

    for uncompiled_dynamic_topology in objects_by_kind.pop(
        DYNAMIC_TOPOLOGY_KIND,
        dict(),
    ).values():
        dynamic_topology: DynamicTopology = fromdict(
            uncompiled_dynamic_topology.data, DynamicTopology
        )

        try:
            definitions.add(dynamic_topology)
        except Exception:
            logging.getLogger(__name__).exception(
                "Failed to build dynamic topology: %s", dynamic_topology.metadata.name
            )

    for object_kind in objects_by_kind.keys():
        raise Exception(f"Unsupported kind {object_kind}")

    return definitions


def load_definitions_from_str(definitions: str) -> StaticDefinitions:
    return compile_static_definitions(load_uncompiled_objects_from_str(definitions))


def load_definitions_from_paths(
    config_dirs: list[str], patches: list[TopologyPatch] | None = None
) -> StaticDefinitions:
    uncompiled_objects = []
    for config_dir in config_dirs:
        uncompiled_objects.extend(load_uncompiled_objects_from_path(config_dir))

    return compile_static_definitions(uncompiled_objects, patches=patches)


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


def merge_with_patches(
    uncompiled_objects: list[UncompiledObject], patches: list[TopologyPatch]
) -> list[UncompiledObject]:
    uncompiled_object_by_kind_and_name = {
        (u.kind, u.name): u for u in uncompiled_objects
    }
    for patch in patches:
        uncompiled_object = uncompiled_object_by_kind_and_name.get(
            (patch.kind, patch.name)
        )
        if not uncompiled_object:
            logging.warning(
                f"Can't find an uncompiled objects to use with patch {patch=}"
            )
            continue

        uncompiled_object.data = dict_utils.deep_merge(
            a=uncompiled_object.data, b=patch.data
        )
    return uncompiled_objects
