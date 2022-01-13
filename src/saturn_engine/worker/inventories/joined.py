import dataclasses
import json
from collections.abc import AsyncIterator
from typing import Any
from typing import NamedTuple
from typing import Optional

from saturn_engine.core.api import InventoryItem
from saturn_engine.worker.services import Services

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
        flatten: bool = False

    def __init__(self, options: Options, services: Services, **kwargs: object) -> None:
        super().__init__(
            options=options,
            services=services,
            batch_size=options.batch_size,
            **kwargs,
        )
        self.flatten: bool = options.flatten

        # This import must be done late since work_factory depends on this module.
        from saturn_engine.worker.work_factory import build_inventory

        self.inventories = []
        for inventory in options.inventories:
            self.inventories.append(
                (inventory.name, build_inventory(inventory, services=services))
            )

    async def iterate(self, after: Optional[str] = None) -> AsyncIterator[Item]:
        ids = json.loads(after) if after else {}

        async for item in self.inventories_iterator(
            inventories=self.inventories, after=ids
        ):
            args = item.args
            if self.flatten:
                args = {}
                for sub_inventory_args in item.args.values():
                    args.update(sub_inventory_args)
            yield Item(id=json.dumps(item.ids), args=args)

    async def inventories_iterator(
        self, *, inventories: list[tuple[str, Inventory]], after: dict[str, str]
    ) -> AsyncIterator[JoinedItems]:
        name, inventory = inventories[0]
        last_id = after.pop(name, None)
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
