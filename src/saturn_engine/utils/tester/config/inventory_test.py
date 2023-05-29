from typing import Optional

from pydantic import dataclasses

from saturn_engine.core import Cursor
from saturn_engine.utils.declarative_config import BaseObject
from saturn_engine.worker.inventories import Item


@dataclasses.dataclass
class InventorySelector:
    inventory: str


@dataclasses.dataclass
class InventoryTestSpec:
    selector: InventorySelector
    items: list[Item]
    limit: Optional[int] = None
    after: Optional[Cursor] = None


@dataclasses.dataclass
class InventoryTest(BaseObject):
    spec: InventoryTestSpec
