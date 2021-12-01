import abc
import asyncio
import dataclasses
from collections.abc import Iterable
from typing import Any
from typing import Optional
from typing import Type

from saturn_engine.utils.options import OptionsSchema

__all__ = ("Item", "Inventory", "BUILTINS")


@dataclasses.dataclass
class Item:
    id: str
    args: dict[str, Any]


class Inventory(OptionsSchema):
    async def next_batch(self, after: Optional[str] = None) -> Iterable[Item]:
        return []


class BlockingInventory(Inventory, abc.ABC):
    async def next_batch(self, after: Optional[str] = None) -> Iterable[Item]:
        return await asyncio.get_event_loop().run_in_executor(
            None,
            self.next_batch_blocking,
            after,
        )

    @abc.abstractmethod
    def next_batch_blocking(self, after: Optional[str] = None) -> Iterable[Item]:
        raise NotImplementedError()


from .dummy import DummyInventory
from .static import StaticInventory

BUILTINS: dict[str, Type[Inventory]] = {
    "DummyInventory": DummyInventory,
    "StaticInventory": StaticInventory,
}
