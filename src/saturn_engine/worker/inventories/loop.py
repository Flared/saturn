import typing as t

import asyncio
import contextlib
import dataclasses
import itertools
import re

from saturn_engine.core.types import Cursor
from saturn_engine.utils.inspect import get_import_name
from saturn_engine.worker.error_handling import get_exception_name
from saturn_engine.worker.error_handling import match_exception_name
from saturn_engine.worker.inventory import Item
from saturn_engine.worker.inventory import IteratorInventory


def open_range(begin: int, to: t.Optional[int]) -> t.Iterator[int]:
    it: t.Iterator[int]
    if to is None:
        it = itertools.count(begin)
    else:
        it = iter(range(begin, to))
    yield from it


class StopLoopInventory(Exception):
    pass


class LoopInventory(IteratorInventory):
    @dataclasses.dataclass
    class Options:
        max_iterations: t.Optional[int] = None
        break_exc_name: str = re.escape(get_import_name(StopLoopInventory))

    def __init__(self, options: Options, **kwargs: t.Any) -> None:
        super().__init__(batch_size=1)
        self.options = options
        self.iterate_lock = asyncio.Lock()
        self.is_running = False
        self.break_exc_name_re = re.compile(self.options.break_exc_name)

    async def iterate(self, after: t.Optional[Cursor] = None) -> t.AsyncIterator[Item]:
        self.is_running = True
        begin = int(after) if after else 0
        for n in open_range(begin, self.options.max_iterations):
            async with contextlib.AsyncExitStack() as stack:
                await stack.enter_async_context(self.process_item())
                if not self.is_running:
                    break
                yield Item(
                    id=str(n),
                    cursor=str(n),
                    args={"iteration": n},
                    context=stack.pop_all(),
                )

    @contextlib.asynccontextmanager
    async def process_item(self) -> t.AsyncIterator[None]:
        async with self.iterate_lock:
            try:
                yield
            except Exception as e:
                exc_name = get_exception_name(e)
                if match_exception_name(self.break_exc_name_re, exc_name):
                    self.stop()
                raise

    def stop(self) -> None:
        self.is_running = False
