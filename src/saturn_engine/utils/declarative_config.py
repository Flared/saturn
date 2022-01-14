import dataclasses
import os
from typing import Optional

import yaml


@dataclasses.dataclass
class ObjectMetadata:
    name: str


@dataclasses.dataclass
class BaseObject:
    metadata: ObjectMetadata
    apiVersion: str
    kind: str


@dataclasses.dataclass
class UncompiledObject:
    api_version: str
    kind: str
    data: dict


def load_uncompiled_objects_from_str(definitions: str) -> list[UncompiledObject]:
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


def load_uncompiled_objects_from_path(path: str) -> list[UncompiledObject]:
    if os.path.isdir(path):
        return load_uncompiled_objects_from_directory(path)
    with open(path, "r", encoding="utf-8") as f:
        return load_uncompiled_objects_from_str(f.read())


def load_uncompiled_objects_from_directory(config_dir: str) -> list[UncompiledObject]:
    uncompiled_objects: list[UncompiledObject] = list()

    for config_file in os.listdir(config_dir):
        if not config_file.endswith(".yaml"):
            continue

        with open(os.path.join(config_dir, config_file), "r", encoding="utf-8") as f:
            uncompiled_objects.extend(load_uncompiled_objects_from_str(f.read()))

    return uncompiled_objects
