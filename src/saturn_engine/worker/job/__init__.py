import abc
from typing import Optional

import asyncstdlib as alib

from saturn_engine.core import TopicMessage
from saturn_engine.utils.log import getLogger
from saturn_engine.utils.options import OptionsSchema

from ..inventories import Inventory
from ..inventories import Item
from ..topics import Topic


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


class Job:
    def __init__(
        self, *, inventory: Inventory, publisher: Topic, store: JobStore
    ) -> None:
        self.logger = getLogger(__name__, self)
        self.inventory = inventory
        self.publisher = publisher
        self.store = store
        self.batch_size = 10

    async def run(self) -> None:
        after = await self.store.load_cursor()
        n = 0
        done = None

        async with alib.scoped_iter(self.inventory.iterate(after=after)) as iterator:
            while done is not True:
                try:
                    done = True
                    async for item in alib.islice(iterator, self.batch_size):
                        after = item.id
                        done = False
                        message = TopicMessage(id=str(item.id), args=item.args)
                        await self.publisher.publish(message, wait=True)
                finally:
                    if not done:
                        await self.store.save_cursor(after=after)

        await self.store.set_completed()
