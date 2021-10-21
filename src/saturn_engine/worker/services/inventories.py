from typing import Optional
from typing import Type

from saturn_engine.worker.context import Context
from saturn_engine.worker.inventories import Inventory
from saturn_engine.worker.inventories.dummy import DummyInventory


class InventoriesService:
    def build(self, *, type_name: str, options: dict, context: Context) -> Inventory:
        cls: Optional[Type[Inventory]] = None
        if type_name == "dummy":
            cls = DummyInventory

        if cls is None:
            raise ValueError("Unknown inventory type")

        return cls.from_options(options)
