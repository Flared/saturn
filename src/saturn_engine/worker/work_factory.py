import asyncio
import dataclasses
import functools
import uuid
from dataclasses import field
from typing import Type
from typing import cast

from saturn_engine.core.api import DummyItem
from saturn_engine.core.api import DummyJob
from saturn_engine.core.api import MemoryItem
from saturn_engine.core.api import QueueItem

from .context import Context
from .job import Job
from .job.memory import MemoryJobStore
from .queues import ExecutableQueue
from .queues import TopicReader
from .queues.dummy import DummyQueue
from .queues.memory import MemoryPublisher
from .queues.memory import MemoryQueue
from .queues.rabbitmq import RabbitMQQueue
from .work import SchedulableQueue
from .work import WorkItems


@dataclasses.dataclass
class GeneratedItem:
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    options: dict = field(default_factory=dict)
    pipeline: dict = field(default_factory=dict)


def init_with_options(
    cls: Type[TopicReader], item: QueueItem, *args: object, **kwargs: object
) -> SchedulableQueue:
    options = {**{"id": item.id}, **item.options}
    queue = cls.from_options(options, *args, **kwargs)
    return SchedulableQueue(ExecutableQueue(queue=queue, pipeline=item.pipeline))


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
            init_with_options(DummyQueue, cast(QueueItem, GeneratedItem()))
            for i in range(queues_count)
        ],
        tasks=[
            init_with_options(DummyQueue, cast(QueueItem, GeneratedItem()))
            for i in range(tasks_count)
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
    queue = ExecutableQueue(
        queue=MemoryQueue(MemoryQueue.Options(id=job_item.id)),
        pipeline=job_item.pipeline,
    )
    store = MemoryJobStore()
    job = Job(inventory=inventory, publisher=publisher, store=store)

    return WorkItems(
        queues=[SchedulableQueue(queue)], tasks=[asyncio.create_task(job.run())]
    )
