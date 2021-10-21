import dataclasses
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager


@dataclasses.dataclass
class Message:
    body: str
    id: str = dataclasses.field(default_factory=lambda: str(uuid.uuid4()))

    @asynccontextmanager
    async def process(self) -> AsyncIterator:
        yield
