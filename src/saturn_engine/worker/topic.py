from typing import AsyncContextManager
from typing import Optional
from typing import Union

import abc
import asyncio
from collections.abc import AsyncGenerator
from datetime import timedelta

from saturn_engine.core import TopicMessage
from saturn_engine.utils.log import getLogger
from saturn_engine.utils.options import OptionsSchema

TopicOutput = Union[AsyncContextManager[TopicMessage], TopicMessage]


class Topic(OptionsSchema):
    name: str

    async def run(self) -> AsyncGenerator[TopicOutput, None]:
        raise NotImplementedError()
        yield

    async def publish(self, message: TopicMessage, wait: bool) -> bool:
        raise NotImplementedError()

    async def close(self) -> None:
        pass


class BlockingTopic(Topic, abc.ABC):
    def __init__(
        self,
        max_concurrency: int = 1,
        sleep_time: Optional[timedelta] = None,
    ):
        self.logger = getLogger(__name__, self)
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self.sleep_time: timedelta = sleep_time or timedelta(seconds=0)

    async def run(self) -> AsyncGenerator[TopicOutput, None]:
        while True:
            try:
                messages = await asyncio.get_event_loop().run_in_executor(
                    None,
                    self.run_once_blocking,
                )
            except Exception:
                self.logger.exception("Topic failed")
                await asyncio.sleep(self.sleep_time.total_seconds())
                continue

            if messages is None:
                break

            for message in messages:
                yield message

            if len(messages) == 0:
                await asyncio.sleep(self.sleep_time.total_seconds())

    async def publish(self, message: TopicMessage, wait: bool) -> bool:
        if not wait and self.semaphore.locked():
            return False

        async with self.semaphore:
            return await asyncio.get_event_loop().run_in_executor(
                None,
                self.publish_blocking,
                message,
                wait,
            )

    @abc.abstractmethod
    def run_once_blocking(self) -> Optional[list[TopicOutput]]:
        raise NotImplementedError()

    @abc.abstractmethod
    def publish_blocking(self, message: TopicMessage, wait: bool) -> bool:
        raise NotImplementedError()
