import dataclasses
from collections.abc import Iterable
from typing import Any
from typing import Optional

from . import Inventory
from . import Item


class StaticInventory(Inventory):
    @dataclasses.dataclass
    class Options:
        items: list[dict[str, Any]]

    def __init__(self, options: Options, **kwargs: object) -> None:
        self.batch_size = 5
        self.items = options.items

    async def next_batch(self, after: Optional[str] = None) -> Iterable[Item]:
        begin = int(after) + 1 if after else 0
        end = begin + self.batch_size
        return [
            Item(id=str(i), args=args)
            for i, args in enumerate(self.items[begin:end], start=begin)
        ]
