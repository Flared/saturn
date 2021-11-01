import dataclasses
import functools
import os
from typing import Optional

import desert
import marshmallow
import yaml

from saturn_engine.core.api import Inventory as CoreInventory


@dataclasses.dataclass
class BaseObject:
    apiVersion: str
    kind: str


@dataclasses.dataclass
class ObjectMetadata:
    name: str


@dataclasses.dataclass
class InventorySpec:
    type: str
    options: dict[str, str]


@dataclasses.dataclass
class Inventory(BaseObject):
    metadata: ObjectMetadata
    spec: InventorySpec

    @classmethod
    @functools.cache
    def schema(cls) -> marshmallow.Schema:
        return desert.schema(cls)

    def to_core_object(self) -> CoreInventory:
        return CoreInventory(
            name=self.metadata.name,
            type=self.spec.type,
            options=self.spec.options,
        )


@dataclasses.dataclass
class StaticDefinitions:
    inventories: list[Inventory]


def load_definitions_from_directory(config_dir: str) -> StaticDefinitions:
    inventories: list[Inventory] = []

    for config_file in os.listdir(config_dir):

        with open(os.path.join(config_dir, config_file), "r", encoding="utf-8") as f:

            for yaml_object in yaml.load_all(f.read(), yaml.SafeLoader):

                if not yaml_object:
                    continue

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
