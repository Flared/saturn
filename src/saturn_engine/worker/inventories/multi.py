from typing import Any
from typing import NamedTuple
from typing import Optional

import abc
import dataclasses
import json
from collections.abc import AsyncIterator

from saturn_engine.core.api import ComponentDefinition
from saturn_engine.worker.services import Services

from . import Inventory
from . import Item
from . import IteratorInventory


class MultiItems(NamedTuple):
    ids: dict[str, str]
    cursors: dict[str, Optional[str]]
    args: dict[str, dict[str, Any]]


class MultiInventory(IteratorInventory, abc.ABC):
    @dataclasses.dataclass
    class Options:
        inventories: list[ComponentDefinition]
        batch_size: int = 10
        flatten: bool = False
        alias: Optional[str] = None

    def __init__(self, options: Options, services: Services, **kwargs: object) -> None:
        super().__init__(
            options=options,
            services=services,
            batch_size=options.batch_size,
            **kwargs,
        )
        self.flatten: bool = options.flatten
        self.alias: Optional[str] = options.alias

        # This import must be done late since work_factory depends on this module.
        from saturn_engine.worker.work_factory import build_inventory

        self.inventories = []
        for inventory in options.inventories:
            self.inventories.append(
                (inventory.name, build_inventory(inventory, services=services))
            )

    async def iterate(self, after: Optional[str] = None) -> AsyncIterator[Item]:
        cursors = json.loads(after) if after else {}

        async for item in self.inventories_iterator(
            inventories=self.inventories, after=cursors
        ):
            args: dict[str, Any] = item.args
            if self.flatten:
                args = {}
                for sub_inventory_args in item.args.values():
                    args.update(sub_inventory_args)
            if self.alias:
                args = {
                    self.alias: args,
                }
            yield Item(
                id=json.dumps(item.ids), cursor=json.dumps(item.cursors), args=args
            )

    @abc.abstractmethod
    async def inventories_iterator(
        self, *, inventories: list[tuple[str, Inventory]], after: dict[str, str]
    ) -> AsyncIterator[MultiItems]:
        raise NotImplementedError()
        yield
