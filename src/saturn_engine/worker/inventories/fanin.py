import typing as t

import dataclasses
import json
from contextlib import AsyncExitStack

import asyncstdlib as alib

from saturn_engine.core.api import ComponentDefinition
from saturn_engine.core.types import Cursor
from saturn_engine.utils import iterators
from saturn_engine.worker.inventory import Item
from saturn_engine.worker.inventory import IteratorInventory
from saturn_engine.worker.services import Services
from saturn_engine.worker.work_factory import build_inventory


class FanIn(IteratorInventory):
    @dataclasses.dataclass
    class Options:
        inputs: list[ComponentDefinition]

    def __init__(self, options: Options, services: Services, **kwargs: object) -> None:
        super().__init__()

        self.inputs = {
            input_def.name: build_inventory(input_def, services=services)
            for input_def in options.inputs
        }

    async def iterate(self, after: t.Optional[Cursor] = None) -> t.AsyncIterator[Item]:
        cursors = json.loads(after) if after else {}

        async with AsyncExitStack() as ctx:
            aiters: list[t.AsyncIterator[tuple[str, Item]]] = []
            for k, inventory in self.inputs.items():
                to_tuple: t.Callable = lambda m, k=k: (k, m)
                i_iter = inventory.iterate(after=cursors.get(k))
                j_iter = alib.map(to_tuple, i_iter)
                k_iter = alib.scoped_iter(j_iter)
                aiters.append(await ctx.enter_async_context(k_iter))

            async for name, message in iterators.fanin(*aiters):
                cursors[name] = message.cursor
                yield dataclasses.replace(message, cursor=Cursor(json.dumps(cursors)))
