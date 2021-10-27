import asyncio
import dataclasses
from collections.abc import AsyncGenerator
from typing import Any

from saturn_engine.core import Message

from . import AckProcessable
from . import Processable
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

    async def run(self) -> AsyncGenerator[Processable, None]:
        queue = get_queue(self.options.id)
        while True:
            message = await queue.get()
            yield AckProcessable(message, ack=queue.task_done)


class MemoryPublisher(Publisher):
    Options = MemoryOptions

    def __init__(self, options: MemoryOptions, **kwargs: Any):
        self.options = options

    async def push(self, message: Message) -> None:
        queue = get_queue(self.options.id)
        await queue.put(message)


def get_queue(queue_id: str, *, maxsize: int = 10) -> asyncio.Queue:
    if queue_id not in _memory_queues:
        _memory_queues[queue_id] = asyncio.Queue(maxsize=maxsize)
    return _memory_queues[queue_id]


async def join_all() -> None:
    for queue in _memory_queues.values():
        await queue.join()
    reset()


def reset() -> None:
    return _memory_queues.clear()
