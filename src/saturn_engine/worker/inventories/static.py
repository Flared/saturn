import typing as t

import dataclasses

from saturn_engine.core import Cursor
from saturn_engine.core import MessageId

from . import Inventory
from . import Item


class StaticInventory(Inventory):
    @dataclasses.dataclass
    class Options:
        items: list[dict[str, t.Optional[t.Any]]]
        repeat: bool = False

    def __init__(self, options: Options, **kwargs: object) -> None:
        self.items = options.items
        self.repeat = options.repeat

    async def next_batch(self, after: t.Optional[Cursor] = None) -> list[Item]:
        begin = int(after) + 1 if after is not None else 0
        if self.repeat and after:
            begin = begin % len(self.items)
        return [
            Item(id=MessageId(str(i)), args=args)
            for i, args in enumerate(self.items[begin:], start=begin)
        ]
