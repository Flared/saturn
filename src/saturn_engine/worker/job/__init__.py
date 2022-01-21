from typing import Optional

import abc
from collections.abc import AsyncGenerator

import asyncstdlib as alib

from saturn_engine.core import TopicMessage
from saturn_engine.utils.log import getLogger
from saturn_engine.utils.options import OptionsSchema

from ..inventories import Inventory
from ..topics import Topic
from ..topics import TopicOutput


class JobStore(OptionsSchema, abc.ABC):
    @abc.abstractmethod
    async def load_cursor(self) -> Optional[str]:
        pass

    @abc.abstractmethod
    async def save_cursor(self, *, after: str) -> None:
        pass

    @abc.abstractmethod
    async def set_completed(self) -> None:
        pass

    @abc.abstractmethod
    async def set_failed(self, error: Exception) -> None:
        pass


class Job(Topic):
    def __init__(self, *, inventory: Inventory, store: JobStore) -> None:
        self.logger = getLogger(__name__, self)
        self.inventory = inventory
        self.store = store
        self.batch_size = 10

    async def run(self) -> AsyncGenerator[TopicOutput, None]:
        try:
            after = await self.store.load_cursor()
            done = None

            async with alib.scoped_iter(
                self.inventory.iterate(after=after)
            ) as iterator:
                while done is not True:
                    try:
                        done = True
                        async for item in alib.islice(iterator, self.batch_size):
                            after = item.id
                            done = False
                            message = TopicMessage(id=str(item.id), args=item.args)
                            yield message
                    finally:
                        if not done and after is not None:
                            await self.store.save_cursor(after=after)

            await self.store.set_completed()
        except Exception as e:
            self.logger.exception("Exception raised from job")
            await self.store.set_failed(e)
