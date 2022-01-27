from collections.abc import AsyncIterator

from . import Inventory
from .multi import MultiInventory
from .multi import MultiItems


class ChainedInventory(MultiInventory):
    async def inventories_iterator(
        self, *, inventories: list[tuple[str, Inventory]], after: dict[str, str]
    ) -> AsyncIterator[MultiItems]:
        start_inventory = 0
        if after:
            for i, (name, _) in enumerate(inventories):
                if name in after:
                    start_inventory = i
                    break

        for name, inventory in inventories[start_inventory:]:
            async for item in inventory.iterate(after.pop(name, None)):
                yield MultiItems(ids={name: item.id}, args={name: item.args})
