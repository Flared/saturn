from typing import Optional
from typing import Type

import marshmallow

from saturn_engine.core.api import DummyItem
from saturn_engine.core.api import JobItem
from saturn_engine.core.api import QueueItem

from . import Queue
from .context import QueueContext
from .dummy import DummyQueue
from .rabbitmq import RabbitMQQueue


def build(queue_item: QueueItem, *, context: QueueContext) -> list[Queue]:
    queue_class: Optional[Type[Queue]] = None
    if isinstance(queue_item, DummyItem):
        queue_class = DummyQueue
    elif isinstance(queue_item, JobItem):
        queue_class = DummyQueue
    else:
        queue_class = RabbitMQQueue

    if not queue_class:
        raise ValueError("Cannot build queue for %s", queue_item)

    options_schema = queue_class.options_schema(unknown=marshmallow.EXCLUDE)
    options = options_schema.load({**{"id": queue_item.id}, **queue_item.options})

    return [queue_class(options, context=context)]
