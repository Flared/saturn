import dataclasses
import functools
import uuid
from dataclasses import field
from typing import Protocol
from typing import Type
from typing import TypeVar

import marshmallow

from saturn_engine.core.api import DummyItem
from saturn_engine.core.api import JobItem
from saturn_engine.core.api import MemoryItem
from saturn_engine.core.api import QueueItem
from saturn_engine.utils.options import OptionsSchema

from .queues.context import QueueContext
from .queues.dummy import DummyQueue
from .queues.memory import MemoryQueue
from .queues.rabbitmq import RabbitMQQueue
from .work import WorkItems

OptionsSchemaT = TypeVar("OptionsSchemaT", bound=OptionsSchema)


class ItemProtocol(Protocol):
    id: str
    options: dict


@dataclasses.dataclass
class GeneratedItem:
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    options: dict = field(default_factory=dict)


def init_with_options(
    cls: Type[OptionsSchemaT], item: ItemProtocol, *args: object, **kwargs: object
) -> OptionsSchemaT:
    options_schema = cls.options_schema(unknown=marshmallow.EXCLUDE)
    options = options_schema.load({**{"id": item.id}, **item.options})

    return cls(*args, options=options, **kwargs)


@functools.singledispatch
def build(queue_item: object, *, context: QueueContext) -> WorkItems:
    raise ValueError("Cannot build queue for %s", queue_item)


@build.register(QueueItem)
def build_queue(queue_item: MemoryItem, *, context: QueueContext) -> WorkItems:
    return WorkItems(
        queues=[init_with_options(RabbitMQQueue, queue_item, context=context)], tasks=[]
    )


@build.register(DummyItem)
def build_dummy(queue_item: DummyItem, *, context: QueueContext) -> WorkItems:
    queues_count = queue_item.options.get("queues_count", 1)
    tasks_count = queue_item.options.get("tasks_count", 0)
    return WorkItems(
        queues=[
            init_with_options(DummyQueue, GeneratedItem()) for i in range(queues_count)
        ],
        tasks=[
            init_with_options(DummyQueue, GeneratedItem()) for i in range(tasks_count)
        ],
    )


@build.register(MemoryItem)
def build_memory(queue_item: MemoryItem, *, context: QueueContext) -> WorkItems:
    return WorkItems(
        queues=[init_with_options(MemoryQueue, queue_item, context=context)], tasks=[]
    )


@build.register(JobItem)
def build_job(queue_item: MemoryItem, *, context: QueueContext) -> WorkItems:
    return WorkItems(
        queues=[init_with_options(MemoryQueue, queue_item, context=context)], tasks=[]
    )
