from collections.abc import AsyncGenerator
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Callable
from typing import TypeVar

from saturn_engine.core import Message
from saturn_engine.utils.options import OptionsSchema

T = TypeVar("T")


class Processable:
    def __init__(self, message: Message):
        self.message = message

    @asynccontextmanager
    async def process(self) -> AsyncIterator[Message]:
        yield self.message


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
    async def run(self) -> AsyncGenerator[Processable, None]:
        for _ in ():
            yield _


class Publisher(OptionsSchema):
    def __init__(self, options: object) -> None:
        pass

    async def push(self, message: Message) -> None:
        pass
