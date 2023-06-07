from typing import Any

import dataclasses
from dataclasses import field

from saturn_engine.core import api
from saturn_engine.utils.declarative_config import BaseObject


@dataclasses.dataclass
class InventorySpec:
    type: str
    options: dict[str, Any] = field(default_factory=dict)


@dataclasses.dataclass
class Inventory(BaseObject):
    spec: InventorySpec

    def to_core_object(self) -> api.ComponentDefinition:
        return api.ComponentDefinition(
            name=self.metadata.name,
            type=self.spec.type,
            options=self.spec.options,
        )
