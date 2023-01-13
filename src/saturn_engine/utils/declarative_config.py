import dataclasses
import os

import marshmallow
import yaml

from saturn_engine.utils.options import schema_for


@dataclasses.dataclass
class ObjectMetadata:
    name: str
    labels: dict[str, str] = dataclasses.field(default_factory=dict)


@dataclasses.dataclass
class BaseObject:
    metadata: ObjectMetadata
    apiVersion: str
    kind: str


@dataclasses.dataclass
class UncompiledObject:
    api_version: str
    kind: str
    name: str
    data: dict


def load_uncompiled_objects_from_str(definitions: str) -> list[UncompiledObject]:
    uncompiled_objects: list[UncompiledObject] = []

    for yaml_object in yaml.load_all(definitions, yaml.SafeLoader):

        if not yaml_object:
            continue

        base_object: BaseObject = schema_for(BaseObject).load(
            data=yaml_object,
            unknown=marshmallow.EXCLUDE,
        )

        if base_object.apiVersion != "saturn.flared.io/v1alpha1":
            raise Exception(
                f"apiVersion was {base_object.apiVersion}, "
                "we only support saturn.flared.io/v1alpha1"
            )

        uncompiled_objects.append(
            UncompiledObject(
                api_version=base_object.apiVersion,
                kind=base_object.kind,
                name=base_object.metadata.name,
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

    for root, _, filenames in os.walk(config_dir, followlinks=True):
        for filename in filenames:
            if not filename.endswith(".yaml"):
                continue

            with open(os.path.join(root, filename), "r", encoding="utf-8") as f:
                uncompiled_objects.extend(load_uncompiled_objects_from_str(f.read()))

    return uncompiled_objects
