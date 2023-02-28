from typing import Type

from ..inventory import BlockingInventory
from ..inventory import BlockingSubInventory
from ..inventory import Inventory
from ..inventory import Item
from ..inventory import IteratorInventory
from ..inventory import SubInventory
from .batching import BatchingInventory
from .chained import ChainedInventory
from .dummy import DummyInventory
from .joined import JoinedInventory
from .joined_sub import JoinedSubInventory
from .periodic import PeriodicInventory
from .static import StaticInventory

__all__ = (
    "Item",
    "Inventory",
    "IteratorInventory",
    "BlockingInventory",
    "SubInventory",
    "BlockingSubInventory",
    "BUILTINS",
)

BUILTINS: dict[str, Type[Inventory]] = {
    "DummyInventory": DummyInventory,
    "StaticInventory": StaticInventory,
    "JoinedInventory": JoinedInventory,
    "JoinedSubInventory": JoinedSubInventory,
    "ChainedInventory": ChainedInventory,
    "PeriodicInventory": PeriodicInventory,
    "BatchingInventory": BatchingInventory,
}
