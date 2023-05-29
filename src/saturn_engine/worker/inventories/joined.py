from collections.abc import AsyncIterator

from saturn_engine.core import Cursor

from . import Inventory
from .multi import MultiInventory
from .multi import MultiItems


class JoinedInventory(MultiInventory):
    async def inventories_iterator(
        self, *, inventories: list[tuple[str, Inventory]], after: dict[str, Cursor]
    ) -> AsyncIterator[MultiItems]:
        name, inventory = inventories[0]
        last_cursor = after.pop(name, None)
        joined_inventories = inventories[1:]

        async for item in inventory.iterate(after=last_cursor):
            if joined_inventories:
                async for joined_item in self.inventories_iterator(
                    inventories=joined_inventories, after=after
                ):
                    if last_cursor is not None:
                        joined_item.cursors[name] = last_cursor
                    joined_item.args[name] = item.args
                    joined_item.ids[name] = item.id
                    yield joined_item

            else:
                yield MultiItems(
                    ids={name: item.id},
                    cursors={name: item.cursor},
                    args={name: item.args},
                )
            last_cursor = item.cursor
