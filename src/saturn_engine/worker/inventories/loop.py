import typing as t

import asyncio
import contextlib
import dataclasses
import itertools

from saturn_engine.core.pipeline import PipelineEvent
from saturn_engine.core.types import Cursor
from saturn_engine.worker.inventory import Item
from saturn_engine.worker.inventory import IteratorInventory
from saturn_engine.worker.services.hooks import PipelineEventsEmitted


def open_range(begin: int, to: t.Optional[int]) -> t.Iterator[int]:
    it: t.Iterator[int]
    if to is None:
        it = itertools.count(begin)
    else:
        it = iter(range(begin, to))
    yield from it


class StopLoopEvent(PipelineEvent):
    pass


class LoopInventory(IteratorInventory):
    @dataclasses.dataclass
    class Options:
        max_iterations: t.Optional[int] = None

    def __init__(self, options: Options, **kwargs: t.Any) -> None:
        super().__init__(batch_size=1)
        self.options = options
        self.iterate_lock = asyncio.Lock()
        self.is_running = False

    async def iterate(self, after: t.Optional[Cursor] = None) -> t.AsyncIterator[Item]:
        self.is_running = True
        begin = int(after) if after else 0
        for n in open_range(begin, self.options.max_iterations):
            async with contextlib.AsyncExitStack() as stack:
                await stack.enter_async_context(self.iterate_lock)
                if not self.is_running:
                    break
                yield Item(
                    id=str(n),
                    cursor=str(n),
                    args={"iteration": n},
                    context=stack.pop_all(),
                    config={
                        "hooks": {
                            "pipeline_events_emitted": [self.on_event],
                        }
                    },
                )

    async def on_event(self, event: PipelineEventsEmitted) -> None:
        if any(isinstance(e, StopLoopEvent) for e in event.events):
            self.stop()

    def stop(self) -> None:
        self.is_running = False
