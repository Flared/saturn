from typing import Type

from ..topic import BlockingTopic
from ..topic import Topic
from ..topic import TopicOutput
from .dummy import DummyTopic
from .file import FileTopic
from .memory import MemoryTopic
from .rabbitmq import RabbitMQTopic
from .deduplicated import DeduplicatedTopic

__all__ = (
    "Topic",
    "TopicOutput",
    "BlockingTopic",
    "BUILTINS",
)

BUILTINS: dict[str, Type[Topic]] = {
    "DummyTopic": DummyTopic,
    "MemoryTopic": MemoryTopic,
    "RabbitMQTopic": RabbitMQTopic,
    "FileTopic": FileTopic,
    "DeduplicatedTopic": DeduplicatedTopic,
}
