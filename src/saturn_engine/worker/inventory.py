from typing import Any
from typing import Optional

import abc
import asyncio
import dataclasses
from collections.abc import AsyncIterator

import asyncstdlib as alib

from saturn_engine.utils.options import OptionsSchema


@dataclasses.dataclass
class Item:
    id: str
    args: dict[str, Any]


class Inventory(abc.ABC, OptionsSchema):
    @abc.abstractmethod
    async def next_batch(self, after: Optional[str] = None) -> list[Item]:
        """Returns a batch of item with id greater than `after`."""
        raise NotImplementedError()

    async def iterate(self, after: Optional[str] = None) -> AsyncIterator[Item]:
        """Returns an iterable that goes over the whole inventory."""
        while True:
            batch = await self.next_batch(after)
            if not batch:
                return
            for item in batch:
                yield item
            after = item.id


class IteratorInventory(Inventory):
    def __init__(self, *, batch_size: Optional[int] = None, **kwargs: object) -> None:
        self.batch_size = batch_size or 10

    async def next_batch(self, after: Optional[str] = None) -> list[Item]:
        batch: list[Item] = await alib.list(
            alib.islice(self.iterate(after=after), self.batch_size)
        )
        return batch

    @abc.abstractmethod
    async def iterate(self, after: Optional[str] = None) -> AsyncIterator[Item]:
        raise NotImplementedError()
        yield


class BlockingInventory(Inventory, abc.ABC):
    async def next_batch(self, after: Optional[str] = None) -> list[Item]:
        return await asyncio.get_event_loop().run_in_executor(
            None,
            self.next_batch_blocking,
            after,
        )

    @abc.abstractmethod
    def next_batch_blocking(self, after: Optional[str] = None) -> list[Item]:
        raise NotImplementedError()
