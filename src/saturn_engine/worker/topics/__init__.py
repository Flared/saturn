import abc
import asyncio
from collections.abc import AsyncGenerator
from typing import AsyncContextManager
from typing import Optional
from typing import Type
from typing import Union

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


class BlockingTopic(Topic, abc.ABC):
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
from .memory import MemoryTopic
from .rabbitmq import RabbitMQTopic

BUILTINS: dict[str, Type[Topic]] = {
    "DummyTopic": DummyTopic,
    "MemoryTopic": MemoryTopic,
    "RabbitMQTopic": RabbitMQTopic,
}
