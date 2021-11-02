import dataclasses
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from aio_pika import IncomingMessage

from saturn_engine.core import TopicMessage
from saturn_engine.utils.log import getLogger

from ..context import Context
from . import QueueMessage
from . import TopicReader


class RabbitMQQueue(TopicReader):
    """A queue that consume message from RabbitMQ"""

    @dataclasses.dataclass
    class Options:
        queue_name: str

    def __init__(self, options: Options, context: Context, **kwargs: object) -> None:
        self.logger = getLogger(__name__, self)
        self.options = options
        self.context = context

    async def run(self) -> AsyncGenerator[QueueMessage, None]:
        self.logger.info("Starting queue %s", self.options.queue_name)
        connection = await self.context.services.rabbitmq.connection
        async with connection.channel() as channel:
            queue = await channel.declare_queue(self.options.queue_name)

            self.logger.info("Processing queue %s", self.options.queue_name)
            async with queue.iterator() as q:
                async for message in q:
                    yield self.message_context(message)

    @asynccontextmanager
    async def message_context(
        self, message: IncomingMessage
    ) -> AsyncIterator[TopicMessage]:
        async with message.process():
            yield TopicMessage(args=json.loads(message.body.decode()))
