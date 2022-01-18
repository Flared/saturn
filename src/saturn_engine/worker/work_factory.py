from typing import Optional

import asyncio

from saturn_engine.core.api import InventoryItem
from saturn_engine.core.api import QueueItem
from saturn_engine.core.api import TopicItem
from saturn_engine.utils import inspect as extra_inspect

from . import inventories
from . import topics
from .inventories import Inventory
from .job import Job
from .queue import ExecutableQueue
from .services import Services
from .topics import Topic
from .topics.memory import MemoryTopic
from .work import SchedulableQueue
from .work import WorkItems


def build(queue_item: QueueItem, *, services: Services) -> WorkItems:
    task: Optional[asyncio.Task] = None
    if isinstance(queue_item.input, TopicItem):
        queue_input = build_topic(queue_item.input, services=services)
    if isinstance(queue_item.input, InventoryItem):
        task, queue_input = build_inventory_job(
            queue_item.input, queue_item=queue_item, services=services
        )

    output = {
        k: [build_topic(t, services=services) for t in ts]
        for k, ts in queue_item.output.items()
    }

    return WorkItems(
        queues=[
            SchedulableQueue(
                ExecutableQueue(
                    topic=queue_input,
                    pipeline=queue_item.pipeline,
                    output=output,
                    services=services,
                )
            )
        ],
        tasks=[task] if task else [],
    )


def build_topic(topic_item: TopicItem, *, services: Services) -> Topic:
    klass = topics.BUILTINS.get(topic_item.type)
    if klass is None:
        klass = extra_inspect.import_name(topic_item.type)
    if klass is None:
        raise ValueError(f"Unknown topic type: {topic_item.type}")
    if not issubclass(klass, Topic):
        raise ValueError(f"{klass} must be a Topic")
    options = {"name": topic_item.name} | topic_item.options
    return klass.from_options(options, services=services)


def build_inventory_job(
    inventory_item: InventoryItem, *, queue_item: QueueItem, services: Services
) -> tuple[asyncio.Task, Topic]:
    inventory_input = build_inventory(inventory_item, services=services)
    inventory_output = MemoryTopic(
        MemoryTopic.Options(name=queue_item.name, buffer_size=1)
    )
    store = services.job_store.for_queue(queue_item)
    job = Job(inventory=inventory_input, publisher=inventory_output, store=store)
    input_topic = MemoryTopic(MemoryTopic.Options(name=queue_item.name, buffer_size=1))
    return (asyncio.create_task(job.run()), input_topic)


def build_inventory(inventory_item: InventoryItem, *, services: Services) -> Inventory:
    klass = inventories.BUILTINS.get(inventory_item.type)
    if klass is None:
        klass = extra_inspect.import_name(inventory_item.type)
    if klass is None:
        raise ValueError(f"Unknown inventory type: {inventory_item.type}")
    if not issubclass(klass, Inventory):
        raise ValueError(f"{klass} must be an Inventory")
    options = {"name": inventory_item.name} | inventory_item.options
    return klass.from_options(options, services=services)
