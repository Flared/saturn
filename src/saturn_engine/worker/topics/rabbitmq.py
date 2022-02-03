from typing import AsyncContextManager

import dataclasses
import json
from collections.abc import AsyncGenerator
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import aio_pika
from aio_pika import IncomingMessage

from saturn_engine.core import TopicMessage
from saturn_engine.utils.log import getLogger
from saturn_engine.utils.options import asdict
from saturn_engine.utils.options import fromdict
from saturn_engine.worker.services import Services
from saturn_engine.worker.services.rabbitmq import RabbitMQService

from . import Topic


class RabbitMQTopic(Topic):
    """A queue that consume message from RabbitMQ"""

    @dataclasses.dataclass
    class Options:
        queue_name: str
        auto_delete: bool
        persistent: bool

    class TopicServices:
        rabbitmq: RabbitMQService

    def __init__(self, options: Options, services: Services, **kwargs: object) -> None:
        self.logger = getLogger(__name__, self)
        self.options = options
        self.services = services.cast(RabbitMQTopic.TopicServices)

    async def run(self) -> AsyncGenerator[AsyncContextManager[TopicMessage], None]:
        self.logger.info("Starting queue %s", self.options.queue_name)
        connection: aio_pika.Connection = await self.services.rabbitmq.connection
        async with connection.channel() as channel:
            self.logger.info("Processing queue %s", self.options.queue_name)
            queue = await channel.declare_queue(
                self.options.queue_name, auto_delete=self.options.auto_delete
            )
            if self.options.persistent:
                exch = await channel.declare_exchange(
                    self.options.queue_name, auto_delete=self.options.auto_delete
                )
                await queue.bind(exch, self.options.queue_name)
            async with queue.iterator() as q:
                async for message in q:
                    yield self.message_context(message)

    async def publish(self, message: TopicMessage, wait: bool) -> bool:
        try:
            connection: aio_pika.Connection = await self.services.rabbitmq.connection
            async with connection.channel(on_return_raises=True) as channel:
                queue = await channel.declare_queue(
                    self.options.queue_name, auto_delete=self.options.auto_delete
                )
                exch = channel.default_exchange
                if self.options.persistent:
                    exch = await channel.declare_exchange(
                        self.options.queue_name, auto_delete=self.options.auto_delete
                    )
                    await queue.bind(exch, self.options.queue_name)
                await exch.publish(
                    aio_pika.Message(body=json.dumps(asdict(message)).encode()),
                    routing_key=self.options.queue_name,
                )
                return True
        except Exception as err:
            self.logger.error(
                f"Error while publishing message id '{message.id}'"
                + f" on queue '{self.options.queue_name}': {err}"
            )
            return False

    @asynccontextmanager
    async def message_context(
        self, message: IncomingMessage
    ) -> AsyncIterator[TopicMessage]:
        async with message.process():
            yield fromdict(json.loads(message.body.decode()), TopicMessage)
