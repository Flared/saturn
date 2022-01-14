from typing import AsyncContextManager
from typing import Optional
from typing import Type
from typing import Union

import abc
import asyncio
from collections.abc import AsyncGenerator

from saturn_engine.core import TopicMessage
from saturn_engine.utils.options import OptionsSchema

__all__ = (
    "Topic",
    "TopicOutput",
    "BUILTINS",
)

TopicOutput = Union[AsyncContextManager[TopicMessage], TopicMessage]


class Topic(OptionsSchema):
    async def run(self) -> AsyncGenerator[TopicOutput, None]:
        raise NotImplementedError()
        yield

    async def publish(self, message: TopicMessage, wait: bool) -> bool:
        raise NotImplementedError()

    async def close(self) -> None:
        pass


class BlockingTopic(Topic, abc.ABC):
    def __init__(self, max_concurrency: int = 1):
        self.semaphore = asyncio.Semaphore(max_concurrency)

    async def run(self) -> AsyncGenerator[TopicOutput, None]:
        while True:
            message = await asyncio.get_event_loop().run_in_executor(
                None,
                self.run_once_blocking,
            )
            if message is None:
                break
            yield message

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

    def run_once_blocking(self) -> Optional[TopicOutput]:
        raise NotImplementedError()

    def publish_blocking(self, message: TopicMessage, wait: bool) -> bool:
        raise NotImplementedError()


# Expose common topic through this module.
from .dummy import DummyTopic
from .file import FileTopic
from .memory import MemoryTopic
from .rabbitmq import RabbitMQTopic

BUILTINS: dict[str, Type[Topic]] = {
    "DummyTopic": DummyTopic,
    "MemoryTopic": MemoryTopic,
    "RabbitMQTopic": RabbitMQTopic,
    "FileTopic": FileTopic,
}
