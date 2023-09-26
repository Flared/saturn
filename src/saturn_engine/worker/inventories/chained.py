import typing as t

import dataclasses
import json
from collections.abc import AsyncIterator

from saturn_engine.core import Cursor
from saturn_engine.core.api import ComponentDefinition
from saturn_engine.worker.inventory import IteratorInventory
from saturn_engine.worker.services import Services

from . import Inventory
from . import Item
from .multi import MultiItems


class CurrentInventory(t.NamedTuple):
    name: str
    inventory: Inventory


class ChainedInventory(IteratorInventory):
    @dataclasses.dataclass
    class Options:
        inventories: list[ComponentDefinition]

        batch_size: int = 10
        flatten: bool = False

        alias: t.Optional[str] = None

    def __init__(self, options: Options, services: Services, **kwargs: object) -> None:
        super().__init__(
            options=options,
            services=services,
            batch_size=options.batch_size,
            **kwargs,
        )
        self.options = options
        self.__services = services
        self.__current: t.Optional[CurrentInventory] = None

    async def iterate(self, after: t.Optional[Cursor] = None) -> AsyncIterator[Item]:
        cursors = json.loads(after) if after else {}
        inventories = self.options.inventories

        self.inventories = [(i.name, self.build_inventory(i)) for i in inventories]

        start_inventory = 0
        if cursors:
            for i, (name, _) in enumerate(self.inventories):
                if name in cursors:
                    start_inventory = i
                    break

        for name, inventory in self.inventories[start_inventory:]:
            self.__current = CurrentInventory(name, inventory)
            async for item in inventory.run(cursors.get(name, None)):
                yield MultiItems.from_one(item, name=name).as_item(
                    flatten=self.options.flatten,
                    alias=self.options.alias,
                    context=item.context,
                )

    def build_inventory(self, inventory: ComponentDefinition) -> Inventory:
        # This import must be done late since work_factory depends on this module.
        from saturn_engine.worker.work_factory import build_inventory

        return build_inventory(inventory, services=self.__services)

    @property
    def cursor(self) -> t.Optional[Cursor]:
        if self.__current is None:
            return None

        return Cursor(
            json.dumps({self.__current.name: self.__current.inventory.cursor})
        )
