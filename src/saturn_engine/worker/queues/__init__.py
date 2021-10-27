import asyncio
from abc import abstractmethod
from collections.abc import AsyncGenerator
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Callable
from typing import Optional
from typing import TypeVar

from saturn_engine.core import Message
from saturn_engine.utils.options import OptionsSchema

T = TypeVar("T")


class Parkers:
    """Parkers allow multiple parker to lock a queue.

    A queue will only unlock once all parker have been unparked. It could happen
    in the case where a pipeline has an item waiting on resource (Blocking the
    queue) and another waiting to publish on an upstream queue (Also blocking
    the queue). Both item might have been dequeued before either block the
    queue. So the parkers keep track of both items and won't resume processing
    of the queue until both are done.
    """

    def __init__(self) -> None:
        self.condition = asyncio.Condition()
        self.parkers: set[object] = set()

    def park(self, parker: object) -> None:
        self.parkers.add(parker)

    async def unpark(self, parker: object) -> None:
        async with self.condition:
            self.parkers.discard(parker)
            self.condition.notify()

    def locked(self) -> bool:
        return bool(self.parkers)

    async def wait(self) -> None:
        """Wait until no parker are left. If no parkers, return right away."""
        async with self.condition:
            await self.condition.wait_for(lambda: not self.parkers)


class Processable:
    def __init__(self, message: Message, parker: Optional[Parkers] = None):
        self.message = message
        self.parker = parker

    @asynccontextmanager
    async def process(self) -> AsyncIterator[Message]:
        yield self.message

    def park(self) -> None:
        if self.parker:
            self.parker.park(id(self))

    async def unpark(self) -> None:
        if self.parker:
            await self.parker.unpark(id(self))


class AckProcessable(Processable):
    def __init__(self, message: Message, *, ack: Callable[[], object]):
        super().__init__(message=message)
        self.ack = ack

    @asynccontextmanager
    async def process(self) -> AsyncIterator[Message]:
        try:
            async with super().process() as message:
                yield message
        finally:
            self.ack()


class Queue(OptionsSchema):
    @abstractmethod
    async def run(self) -> AsyncGenerator[Processable, None]:
        for _ in ():
            yield _


class BlockingQueue:
    def __init__(self, queue: Queue):
        self.queue = queue
        self.parkers = Parkers()

    async def run(self) -> AsyncGenerator[Processable, None]:
        async for processable in self.queue.run():
            await self.parkers.wait()
            processable.parker = self.parkers
            yield processable


class Publisher(OptionsSchema):
    def __init__(self, options: object) -> None:
        pass

    async def push(self, message: Message) -> None:
        pass
