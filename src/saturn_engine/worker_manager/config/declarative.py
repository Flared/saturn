import dataclasses
import os
from collections import defaultdict
from typing import DefaultDict
from typing import Optional

import yaml

from saturn_engine.utils.options import fromdict

from .declarative_inventory import Inventory
from .declarative_job_definition import JobDefinition
from .declarative_resource import Resource
from .declarative_topic_item import TopicItem
from .static_definitions import StaticDefinitions


@dataclasses.dataclass
class UncompiledObject:
    api_version: str
    kind: str
    data: dict


def _load_uncompiled_objects_from_str(definitions: str) -> list[UncompiledObject]:
    uncompiled_objects: list[UncompiledObject] = []

    for yaml_object in yaml.load_all(definitions, yaml.SafeLoader):

        if not yaml_object:
            continue

        api_version: Optional[str] = yaml_object.get("apiVersion")
        if not api_version:
            raise Exception("Missing apiVersion")
        elif api_version != "saturn.flared.io/v1alpha1":
            raise Exception(
                f"apiVersion was {api_version}, "
                "we only support saturn.flared.io/v1alpha1"
            )

        object_kind: Optional[str] = yaml_object.get("kind")

        if not object_kind:
            raise Exception("All objects should have a kind")

        uncompiled_objects.append(
            UncompiledObject(
                api_version=api_version,
                kind=object_kind,
                data=yaml_object,
            )
        )

    return uncompiled_objects


def _compile_objects(uncompiled_objects: list[UncompiledObject]) -> StaticDefinitions:
    objects_by_kind: DefaultDict[str, list[UncompiledObject]] = defaultdict(list)
    for uncompiled_object in uncompiled_objects:
        objects_by_kind[uncompiled_object.kind].append(uncompiled_object)

    definitions: StaticDefinitions = StaticDefinitions()

    for uncompiled_inventory in objects_by_kind.pop("SaturnInventory", list()):
        inventory: Inventory = fromdict(uncompiled_inventory.data, Inventory)
        definitions.inventories[inventory.metadata.name] = inventory.to_core_object()

    for uncompiled_topic in objects_by_kind.pop("SaturnTopic", list()):
        topic_item: TopicItem = fromdict(uncompiled_topic.data, TopicItem)
        definitions.topics[topic_item.metadata.name] = topic_item.to_core_object()

    for uncompiled_job_definition in objects_by_kind.pop("SaturnJobDefinition", list()):
        job_definition: JobDefinition = fromdict(
            uncompiled_job_definition.data, JobDefinition
        )
        definitions.job_definitions[
            job_definition.metadata.name
        ] = job_definition.to_core_object(definitions)

    for uncompiled_resource in objects_by_kind.pop("SaturnResource", list()):
        resource: Resource = fromdict(uncompiled_resource.data, Resource)
        resource_item = resource.to_core_object()
        definitions.resources[resource.metadata.name] = resource_item
        definitions.resources_by_type[resource_item.type].append(resource_item)

    for object_kind in objects_by_kind.keys():
        raise Exception(f"Unsupported kind {object_kind}")

    return definitions


def load_definitions_from_str(definitions: str) -> StaticDefinitions:
    return _compile_objects(_load_uncompiled_objects_from_str(definitions))


def load_definitions_from_directory(config_dir: str) -> StaticDefinitions:
    uncompiled_objects: list[UncompiledObject] = list()

    for config_file in os.listdir(config_dir):
        if not config_file.endswith(".yaml"):
            continue

        with open(os.path.join(config_dir, config_file), "r", encoding="utf-8") as f:
            uncompiled_objects.extend(_load_uncompiled_objects_from_str(f.read()))

    return _compile_objects(uncompiled_objects)
