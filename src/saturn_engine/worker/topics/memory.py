from typing import Any
from typing import AsyncContextManager

import asyncio
import dataclasses
from collections.abc import AsyncGenerator
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from saturn_engine.core import TopicMessage

from . import Topic

_memory_queues: dict[str, asyncio.Queue] = {}


@dataclasses.dataclass
class MemoryOptions:
    name: str
    buffer_size: int = 100


class MemoryTopic(Topic):
    name = "memory_topic"

    Options = MemoryOptions

    def __init__(self, options: MemoryOptions, **kwargs: Any):
        self.options = options

    async def run(self) -> AsyncGenerator[AsyncContextManager[TopicMessage], None]:
        queue = get_queue(self.options.name, maxsize=self.options.buffer_size)
        while True:
            message = await queue.get()
            yield self.message_context(message, queue=queue)

    @asynccontextmanager
    async def message_context(
        self, message: TopicMessage, queue: asyncio.Queue
    ) -> AsyncIterator[TopicMessage]:
        try:
            yield message
        finally:
            queue.task_done()

    async def publish(self, message: TopicMessage, wait: bool) -> bool:
        queue = get_queue(self.options.name, maxsize=self.options.buffer_size)
        if wait:
            await queue.put(message)
        else:
            try:
                queue.put_nowait(message)
                return True
            except asyncio.QueueFull:
                return False
        return True


def get_queue(queue_id: str, *, maxsize: int = 100) -> asyncio.Queue:
    if queue_id not in _memory_queues:
        _memory_queues[queue_id] = asyncio.Queue(maxsize=maxsize)
    return _memory_queues[queue_id]


async def join_all() -> None:
    for queue in _memory_queues.values():
        await queue.join()
    reset()


def reset() -> None:
    return _memory_queues.clear()
