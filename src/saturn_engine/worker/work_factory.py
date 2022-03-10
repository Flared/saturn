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
from .work_item import WorkItem


def build(queue_item: QueueItem, *, services: Services) -> WorkItem:
    if isinstance(queue_item.input, TopicItem):
        topic = build_topic(queue_item.input, services=services)
    if isinstance(queue_item.input, InventoryItem):
        topic = build_inventory_job(
            queue_item.input, queue_item=queue_item, services=services
        )

    output = {
        k: [build_topic(t, services=services) for t in ts]
        for k, ts in queue_item.output.items()
    }

    executable_queue = ExecutableQueue(
        topic=topic,
        pipeline=queue_item.pipeline,
        output=output,
        services=services,
    )
    return WorkItem(iterable=executable_queue.run(), name=queue_item.name)


def build_topic(topic_item: TopicItem, *, services: Services) -> Topic:
    klass = topics.BUILTINS.get(topic_item.type)
    if klass is None:
        klass = extra_inspect.import_name(topic_item.type)
    if klass is None:
        raise ValueError(f"Unknown topic type: {topic_item.type}")
    if not issubclass(klass, Topic):
        raise ValueError(f"{klass} must be a Topic")
    options = {"name": topic_item.name} | topic_item.options
    topic = klass.from_options(options, services=services)
    topic.name = topic_item.name
    return topic


def build_inventory_job(
    inventory_item: InventoryItem, *, queue_item: QueueItem, services: Services
) -> Job:
    inventory = build_inventory(inventory_item, services=services)
    store = services.job_store.for_queue(queue_item)
    return Job(inventory=inventory, store=store)


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
