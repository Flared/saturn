import dataclasses
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager


@dataclasses.dataclass
class Message:
    body: str

    @asynccontextmanager
    async def process(self) -> AsyncIterator:
        yield
