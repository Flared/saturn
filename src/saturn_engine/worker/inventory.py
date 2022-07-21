import typing as t

import abc
import asyncio
import dataclasses
import logging
import uuid
from collections.abc import AsyncIterator
from datetime import timedelta
from functools import cached_property

import asyncstdlib as alib

from saturn_engine.utils.options import OptionsSchema


@dataclasses.dataclass
class Item:
    args: dict[str, t.Any]
    id: str = dataclasses.field(default_factory=lambda: str(uuid.uuid4()))
    cursor: str = None  # type: ignore[assignment]
    tags: dict[str, str] = dataclasses.field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.cursor is None:
            self.cursor = self.id


class MaxRetriesError(Exception):
    pass


class RetryBatch(Exception):
    def __init__(self, *, delay: t.Optional[timedelta], max_retries: int = 1) -> None:
        super().__init__(delay)
        self.delay = delay
        self.max_retries = max_retries

    async def wait_delay(self) -> None:
        if self.delay:
            await asyncio.sleep(self.delay.total_seconds())

    def check_max_retries(self, retries_count: int) -> None:
        if retries_count >= self.max_retries:
            raise MaxRetriesError() from self.__cause__


class Inventory(abc.ABC, OptionsSchema):
    @abc.abstractmethod
    async def next_batch(self, after: t.Optional[str] = None) -> list[Item]:
        """Returns a batch of item with id greater than `after`."""
        raise NotImplementedError()

    async def iterate(self, after: t.Optional[str] = None) -> AsyncIterator[Item]:
        """Returns an iterable that goes over the whole inventory."""
        retries_count = 0
        while True:
            try:
                batch = await self.next_batch(after)
            except RetryBatch as e:
                e.check_max_retries(retries_count)
                if e.__cause__:
                    self.logger.error("Retrying to get batch", exc_info=e)
                await e.wait_delay()
                retries_count += 1
                continue
            else:
                retries_count = 0

            if not batch:
                return
            for item in batch:
                yield item
            after = item.cursor

    @cached_property
    def logger(self) -> logging.Logger:
        return logging.getLogger(__name__ + ".Inventory")


class IteratorInventory(Inventory):
    def __init__(self, *, batch_size: t.Optional[int] = None, **kwargs: object) -> None:
        self.batch_size = batch_size or 10

    async def next_batch(self, after: t.Optional[str] = None) -> list[Item]:
        batch: list[Item] = await alib.list(
            alib.islice(self.iterate(after=after), self.batch_size)
        )
        return batch

    @abc.abstractmethod
    async def iterate(self, after: t.Optional[str] = None) -> AsyncIterator[Item]:
        raise NotImplementedError()
        yield


class BlockingInventory(Inventory, abc.ABC):
    async def next_batch(self, after: t.Optional[str] = None) -> list[Item]:
        return await asyncio.get_event_loop().run_in_executor(
            None,
            self.next_batch_blocking,
            after,
        )

    @abc.abstractmethod
    def next_batch_blocking(self, after: t.Optional[str] = None) -> list[Item]:
        raise NotImplementedError()
