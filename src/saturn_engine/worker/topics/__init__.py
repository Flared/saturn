from collections.abc import AsyncGenerator
from typing import AsyncContextManager
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

    async def push(self, message: TopicMessage) -> None:
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
