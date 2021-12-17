import dataclasses
import json
from collections.abc import Iterable
from itertools import product
from typing import Any
from typing import NamedTuple
from typing import Optional

import asyncstdlib as alib

from saturn_engine.core.api import InventoryItem
from saturn_engine.worker.context import Context

from . import Inventory
from . import Item
from . import IteratorInventory


class JoinedItems(NamedTuple):
    ids: dict[str, str]
    args: dict[str, Any]


class JoinedInventory(IteratorInventory):
    @dataclasses.dataclass
    class Options:
        inventories: list[InventoryItem]
        batch_size: int = 10

    def __init__(self, options: Options, context: Context, **kwargs: object) -> None:
        # This import must be done late since work_factory depends on this module.
        from saturn_engine.worker.work_factory import build_inventory

        self.batch_size = options.batch_size
        self.inventories = []
        for inventory in options.inventories:
            self.inventories.append(
                (inventory.name, build_inventory(inventory, context=context))
            )

    async def iterate(self, after: Optional[str] = None) -> Iterable[Item]:
        ids = json.loads(after) if after else {}

        async for item in self.inventories_iterator(
            inventories=self.inventories, after=ids
        ):
            yield Item(id=json.dumps(item.ids), args=item.args)

    async def inventories_iterator(
        self, *, inventories: list[Inventory], after: dict[str, str]
    ) -> JoinedItems:
        name, inventory = inventories[0]
        last_id = after.get(name)
        joined_inventories = inventories[1:]

        async for item in inventory.iterate(after=last_id):
            if joined_inventories:
                async for joined_item in self.inventories_iterator(
                    inventories=joined_inventories, after=after
                ):
                    if last_id is not None:
                        joined_item.ids[name] = last_id
                    joined_item.args[name] = item.args
                    yield joined_item

            else:
                yield JoinedItems(ids={name: item.id}, args={name: item.args})
            last_id = item.id
