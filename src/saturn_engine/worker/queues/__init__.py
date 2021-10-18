import dataclasses
from collections.abc import AsyncGenerator
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Callable
from typing import TypeVar

from saturn_engine.core import Message
from saturn_engine.utils.options import OptionsSchema

T = TypeVar("T")


class AckWrapper(Message):
    def __init__(self, message: Message, *, ack: Callable[[], object]):
        super().__init__(**dataclasses.asdict(message))
        self.ack = ack

    @asynccontextmanager
    async def process(self) -> AsyncIterator:
        try:
            yield
        finally:
            self.ack()


class Queue(OptionsSchema):
    async def run(self) -> AsyncGenerator[Message, None]:
        for _ in ():
            yield _


class Publisher(OptionsSchema):
    def __init__(self, options: object) -> None:
        pass

    async def push(self, message: Message) -> None:
        pass
