from collections.abc import AsyncIterator

from . import Inventory
from .multi import MultiInventory
from .multi import MultiItems


class JoinedInventory(MultiInventory):
    async def inventories_iterator(
        self, *, inventories: list[tuple[str, Inventory]], after: dict[str, str]
    ) -> AsyncIterator[MultiItems]:
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
                yield MultiItems(ids={name: item.id}, args={name: item.args})
            last_id = item.id
