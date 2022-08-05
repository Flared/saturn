from saturn_engine.core.api import InventoryItem
from saturn_engine.core.api import QueueItem
from saturn_engine.core.api import ResourcesProviderItem
from saturn_engine.core.api import TopicItem
from saturn_engine.utils import inspect as extra_inspect
from saturn_engine.worker.resources.provider import (
    BUILTINS as resources_provider_builtins,
)
from saturn_engine.worker.resources.provider import ResourcesProvider

from . import inventories
from . import topics
from .executors.executable import ExecutableQueue
from .inventories import Inventory
from .job import Job
from .services import Services
from .services.job_store import JobStoreService
from .topics import Topic


def build(queue_item: QueueItem, *, services: Services) -> ExecutableQueue:
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

    return ExecutableQueue(
        name=queue_item.name,
        executor=queue_item.executor,
        topic=topic,
        pipeline=queue_item.pipeline,
        output=output,
        services=services,
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
    topic = klass.from_options(options, services=services)
    topic.name = topic_item.name
    return topic


def build_inventory_job(
    inventory_item: InventoryItem, *, queue_item: QueueItem, services: Services
) -> Job:
    inventory = build_inventory(inventory_item, services=services)
    store = services.cast_service(JobStoreService).for_queue(queue_item)
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


def build_resources_provider(
    item: ResourcesProviderItem, *, services: Services
) -> ResourcesProvider:
    klass = resources_provider_builtins.get(item.type)
    if klass is None:
        klass = extra_inspect.import_name(item.type)
    if klass is None:
        raise ValueError(f"Unknown resources provider type: {item.type}")
    if not issubclass(klass, ResourcesProvider):
        raise ValueError(f"{klass} must be a ResourcesProvider")
    options = {"name": item.name} | item.options
    return klass.from_options(options, services=services, definition=item)
