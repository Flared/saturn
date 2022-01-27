from typing import Type

from ..inventory import BlockingInventory
from ..inventory import Inventory
from ..inventory import Item
from ..inventory import IteratorInventory
from .chained import ChainedInventory
from .dummy import DummyInventory
from .joined import JoinedInventory
from .static import StaticInventory

__all__ = ("Item", "Inventory", "IteratorInventory", "BlockingInventory", "BUILTINS")

BUILTINS: dict[str, Type[Inventory]] = {
    "DummyInventory": DummyInventory,
    "StaticInventory": StaticInventory,
    "JoinedInventory": JoinedInventory,
    "ChainedInventory": ChainedInventory,
}
