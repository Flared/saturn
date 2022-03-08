from typing import Type

from ..topic import BlockingTopic
from ..topic import Topic
from ..topic import TopicOutput
from .dummy import DummyTopic
from .file import FileTopic
from .memory import MemoryTopic
from .periodic import PeriodicTopic
from .rabbitmq import RabbitMQTopic
from .static import StaticTopic

__all__ = (
    "Topic",
    "TopicOutput",
    "BlockingTopic",
    "BUILTINS",
)

BUILTINS: dict[str, Type[Topic]] = {
    "DummyTopic": DummyTopic,
    "FileTopic": FileTopic,
    "MemoryTopic": MemoryTopic,
    "PeriodicTopic": PeriodicTopic,
    "RabbitMQTopic": RabbitMQTopic,
    "StaticTopic": StaticTopic,
}
