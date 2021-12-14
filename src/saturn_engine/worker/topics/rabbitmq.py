import dataclasses
import json
from collections.abc import AsyncGenerator
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from aio_pika import IncomingMessage

from saturn_engine.core import TopicMessage
from saturn_engine.utils.log import getLogger
from saturn_engine.worker.services import Services
from saturn_engine.worker.services.rabbitmq import RabbitMQService

from . import Topic
from . import TopicOutput


class RabbitMQTopic(Topic):
    """A queue that consume message from RabbitMQ"""

    @dataclasses.dataclass
    class Options:
        queue_name: str

    class TopicServices:
        rabbitmq: RabbitMQService

    def __init__(self, options: Options, services: Services, **kwargs: object) -> None:
        self.logger = getLogger(__name__, self)
        self.options = options
        self.services = services.cast(RabbitMQTopic.TopicServices)

    async def run(self) -> AsyncGenerator[TopicOutput, None]:
        self.logger.info("Starting queue %s", self.options.queue_name)
        connection = await self.services.rabbitmq.connection
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
