import typing as t

import abc
import dataclasses
import json
from collections.abc import AsyncIterator

from saturn_engine.core import Cursor
from saturn_engine.core import MessageId
from saturn_engine.core.api import ComponentDefinition
from saturn_engine.worker.services import Services

from . import Item
from . import IteratorInventory


class JoinedSubInventory(IteratorInventory, abc.ABC):
    @dataclasses.dataclass
    class Options:
        inventory: ComponentDefinition
        sub_inventory: ComponentDefinition
        batch_size: int = 10
        flatten: bool = False
        alias: t.Optional[str] = None

    def __init__(self, options: Options, services: Services, **kwargs: object) -> None:
        super().__init__(
            options=options,
            services=services,
            batch_size=options.batch_size,
            **kwargs,
        )
        self.flatten: bool = options.flatten
        self.alias: t.Optional[str] = options.alias
        self.inventory_name = options.inventory.name
        self.sub_inventory_name = options.sub_inventory.name

        # This imports must be done late since work_factory depends on this module.
        from saturn_engine.worker.work_factory import build_inventory
        from saturn_engine.worker.work_factory import build_sub_inventory

        self.inventory = build_inventory(options.inventory, services=services)
        self.sub_inventory = build_sub_inventory(
            options.sub_inventory, services=services
        )

    async def iterate(self, after: t.Optional[Cursor] = None) -> AsyncIterator[Item]:
        cursors = json.loads(after) if after else {}
        inventory_cursor = cursors.get(self.inventory_name)
        sub_inventory_cursor = cursors.get(self.sub_inventory_name)

        async for item in self.inventory.iterate(after=inventory_cursor):
            async for sub_item in self.sub_inventory.iterate(
                source_item=item, after=sub_inventory_cursor
            ):
                args: dict[str, t.Any] = {
                    self.inventory_name: item.args,
                    self.sub_inventory_name: sub_item.args,
                }

                if self.flatten:
                    args = {
                        k: v for sub_dict in args.values() for k, v in sub_dict.items()
                    }

                if self.alias:
                    args = {
                        self.alias: args,
                    }

                yield Item(
                    id=MessageId(
                        json.dumps(
                            {
                                self.inventory_name: item.id,
                                self.sub_inventory_name: sub_item.id,
                            }
                        )
                    ),
                    cursor=Cursor(
                        json.dumps(
                            {
                                **(
                                    {self.inventory_name: inventory_cursor}
                                    if inventory_cursor
                                    else {}
                                ),
                                **(
                                    {self.sub_inventory_name: sub_item.cursor}
                                    if sub_item.cursor
                                    else {}
                                ),
                            }
                        )
                    ),
                    args=args,
                )

            inventory_cursor = item.cursor
            sub_inventory_cursor = None
