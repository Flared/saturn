import asyncio
import dataclasses
from collections.abc import AsyncGenerator
from typing import Any

from saturn_engine.core import Message

from . import AckWrapper
from . import Publisher
from . import Queue

_memory_queues: dict[str, asyncio.Queue] = {}


@dataclasses.dataclass
class MemoryOptions:
    id: str


class MemoryQueue(Queue):
    Options = MemoryOptions

    def __init__(self, options: MemoryOptions, **kwargs: Any):
        self.options = options

    async def run(self) -> AsyncGenerator[Message, None]:
        if self.options.id not in _memory_queues:
            _memory_queues[self.options.id] = asyncio.Queue()
        queue = _memory_queues[self.options.id]

        while True:
            message = await queue.get()
            yield AckWrapper(message, ack=queue.task_done)


class MemoryPublisher(Publisher):
    Options = MemoryOptions

    def __init__(self, options: MemoryOptions, **kwargs: Any):
        self.options = options

    async def push(self, message: Message) -> None:
        if self.options.id not in _memory_queues:
            _memory_queues[self.options.id] = asyncio.Queue()
        queue = _memory_queues[self.options.id]
        await queue.put(message)


async def join_all() -> None:
    for queue in _memory_queues.values():
        await queue.join()
