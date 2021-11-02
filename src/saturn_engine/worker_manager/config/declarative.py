import dataclasses
import functools
import os
from typing import Any
from typing import Optional

import desert
import marshmallow
import yaml

from saturn_engine.core.api import InventoryItem


@dataclasses.dataclass
class ObjectMetadata:
    name: str


@dataclasses.dataclass
class BaseObject:
    metadata: ObjectMetadata
    apiVersion: str
    kind: str


@dataclasses.dataclass
class InventorySpec:
    type: str
    options: dict[str, Any]


@dataclasses.dataclass
class Inventory(BaseObject):
    spec: InventorySpec

    @classmethod
    @functools.cache
    def schema(cls) -> marshmallow.Schema:
        return desert.schema(cls)

    def to_core_object(self) -> InventoryItem:
        return InventoryItem(
            name=self.metadata.name,
            type=self.spec.type,
            options=self.spec.options,
        )


@dataclasses.dataclass
class StaticDefinitions:
    inventories: list[Inventory]


def load_definitions_from_str(definitions: str) -> StaticDefinitions:
    inventories: list[Inventory] = []

    for yaml_object in yaml.load_all(definitions, yaml.SafeLoader):

        if not yaml_object:
            continue

        apiVersion: Optional[str] = yaml_object.get("apiVersion")
        if not apiVersion:
            raise Exception("Missing apiVersion")
        elif apiVersion != "saturn.github.io/v1alpha1":
            raise Exception(
                f"apiVersion was {apiVersion},"
                "we only support saturn.github.io/v1alpha1"
            )

        object_kind: Optional[str] = yaml_object.get("kind")
        if not object_kind:
            raise Exception("All objects should have a kind")
        elif object_kind == "SaturnInventory":
            inventories.append(Inventory.schema().load(yaml_object))
        else:
            raise Exception(f"Unknown kind {object_kind}")

    return StaticDefinitions(
        inventories=inventories,
    )


def load_definitions_from_directory(config_dir: str) -> StaticDefinitions:
    inventories: list[Inventory] = []

    for config_file in os.listdir(config_dir):
        with open(os.path.join(config_dir, config_file), "r", encoding="utf-8") as f:
            definitions: StaticDefinitions = load_definitions_from_str(f.read())
            inventories.extend(definitions.inventories)

    return StaticDefinitions(
        inventories=inventories,
    )
