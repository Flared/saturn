from typing import Type

from ..inventory import BlockingInventory
from ..inventory import Inventory
from ..inventory import Item
from ..inventory import IteratorInventory
from .batching import BatchingInventory
from .chained import ChainedInventory
from .dummy import DummyInventory
from .joined import JoinedInventory
from .periodic import PeriodicInventory
from .static import StaticInventory

__all__ = ("Item", "Inventory", "IteratorInventory", "BlockingInventory", "BUILTINS")

BUILTINS: dict[str, Type[Inventory]] = {
    "DummyInventory": DummyInventory,
    "StaticInventory": StaticInventory,
    "JoinedInventory": JoinedInventory,
    "ChainedInventory": ChainedInventory,
    "PeriodicInventory": PeriodicInventory,
    "BatchingInventory": BatchingInventory,
}
