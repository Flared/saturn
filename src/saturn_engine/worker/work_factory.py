import asyncio
import dataclasses
import functools
import uuid
from dataclasses import field
from typing import Protocol
from typing import Type
from typing import TypeVar

from saturn_engine.core.api import DummyItem
from saturn_engine.core.api import DummyJob
from saturn_engine.core.api import MemoryItem
from saturn_engine.core.api import QueueItem
from saturn_engine.utils.options import OptionsSchema

from .context import Context
from .job import Job
from .job.memory import MemoryJobStore
from .queues.dummy import DummyQueue
from .queues.memory import MemoryPublisher
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
    options = {**{"id": item.id}, **item.options}
    return cls.from_options(options, *args, **kwargs)


@functools.singledispatch
def build(queue_item: object, *, context: Context) -> WorkItems:
    raise ValueError("Cannot build queue for %s", queue_item)


@build.register(QueueItem)
def build_queue(queue_item: MemoryItem, *, context: Context) -> WorkItems:
    return WorkItems(
        queues=[init_with_options(RabbitMQQueue, queue_item, context=context)], tasks=[]
    )


@build.register(MemoryItem)
def build_memory(queue_item: MemoryItem, *, context: Context) -> WorkItems:
    return WorkItems(
        queues=[init_with_options(MemoryQueue, queue_item, context=context)], tasks=[]
    )


@build.register(DummyItem)
def build_dummy(queue_item: DummyItem, *, context: Context) -> WorkItems:
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


@build.register(DummyJob)
def build_job(job_item: DummyJob, *, context: Context) -> WorkItems:
    inventory = context.services.inventories.build(
        type_name=job_item.inventory.type,
        options=job_item.inventory.options,
        context=context,
    )
    publisher = MemoryPublisher(MemoryPublisher.Options(id=job_item.id))
    queue = MemoryQueue(MemoryQueue.Options(id=job_item.id))
    store = MemoryJobStore()
    job = Job(inventory=inventory, publisher=publisher, store=store)

    return WorkItems(queues=[queue], tasks=[asyncio.create_task(job.run())])
