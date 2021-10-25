import dataclasses
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from aio_pika import IncomingMessage

from saturn_engine.core import Message
from saturn_engine.utils.log import getLogger

from ..context import Context
from . import Queue


class MessageWrapper(Message):
    def __init__(self, message: IncomingMessage):
        super().__init__(body=message.body.decode())
        self._rmq_message = message

    @asynccontextmanager
    async def process(self) -> AsyncIterator:
        async with self._rmq_message.process():
            yield


class RabbitMQQueue(Queue):
    """A queue that consume message from RabbitMQ"""

    @dataclasses.dataclass
    class Options:
        queue_name: str

    def __init__(self, options: Options, context: Context) -> None:
        self.logger = getLogger(__name__, self)
        self.options = options
        self.context = context

    async def run(self) -> AsyncGenerator[Message, None]:
        self.logger.info("Starting queue %s", self.options.queue_name)
        connection = await self.context.services.rabbitmq.connection
        async with connection.channel() as channel:
            queue = await channel.declare_queue(self.options.queue_name)

            self.logger.info("Processing queue %s", self.options.queue_name)
            async with queue.iterator() as q:
                async for message in q:
                    yield MessageWrapper(message)
