import asyncio
from typing import Optional

from saturn_engine.core.api import InventoryItem
from saturn_engine.core.api import QueueItem
from saturn_engine.core.api import TopicItem
from saturn_engine.utils import inspect as extra_inspect

from . import inventories
from . import topics
from .context import Context
from .inventories import Inventory
from .job import Job
from .queue import ExecutableQueue
from .topics import Topic
from .topics.memory import MemoryTopic
from .work import SchedulableQueue
from .work import WorkItems


def build(queue_item: QueueItem, *, context: Context) -> WorkItems:
    task: Optional[asyncio.Task] = None
    if isinstance(queue_item.input, TopicItem):
        queue_input = build_topic(queue_item.input, context=context)
    if isinstance(queue_item.input, InventoryItem):
        task, queue_input = build_inventory(
            queue_item.input, queue_item=queue_item, context=context
        )

    output = {
        k: [build_topic(t, context=context) for t in ts]
        for k, ts in queue_item.output.items()
    }

    return WorkItems(
        queues=[
            SchedulableQueue(
                ExecutableQueue(
                    topic=queue_input, pipeline=queue_item.pipeline, output=output
                )
            )
        ],
        tasks=[task] if task else [],
    )


def build_topic(topic_item: TopicItem, *, context: Context) -> Topic:
    klass = topics.BUILTINS.get(topic_item.type)
    if klass is None:
        klass = extra_inspect.import_name(topic_item.type)
    if klass is None:
        raise ValueError(f"Unknown topic type: {topic_item.type}")
    if not issubclass(klass, Topic):
        raise ValueError(f"{klass} must be a Topic")
    options = {"name": topic_item.name} | topic_item.options
    return klass.from_options(options, context=context)


def build_inventory(
    inventory_item: InventoryItem, *, queue_item: QueueItem, context: Context
) -> tuple[asyncio.Task, Topic]:
    klass = inventories.BUILTINS.get(inventory_item.type)
    if klass is None:
        klass = extra_inspect.import_name(inventory_item.type)
    if klass is None:
        raise ValueError(f"Unknown inventory type: {inventory_item.type}")
    if not issubclass(klass, Inventory):
        raise ValueError(f"{klass} must be an Inventory")
    options = {"name": inventory_item.name} | inventory_item.options

    inventory_input = klass.from_options(options, context=context)
    inventory_output = MemoryTopic(MemoryTopic.Options(name=queue_item.name))
    store = context.services.job_store.for_queue(queue_item)
    job = Job(inventory=inventory_input, publisher=inventory_output, store=store)
    input_topic = MemoryTopic(MemoryTopic.Options(name=queue_item.name))
    return (asyncio.create_task(job.run()), input_topic)
