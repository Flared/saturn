import dataclasses
from typing import AsyncGenerator

from saturn_engine.core import Message
from saturn_engine.utils.log import getLogger

from . import Queue
from .context import QueueContext


class RabbitMQQueue(Queue):
    """A queue that consume message from RabbitMQ"""

    @dataclasses.dataclass
    class Options:
        queue_name: str

    def __init__(self, options: Options, context: QueueContext) -> None:
        self.logger = getLogger(__name__, self)
        self.options = options
        self.context = context

    async def run(self) -> AsyncGenerator[Message, None]:
        breakpoint()
        self.logger.info("Starting queue %s", self.options.queue_name)
        connection = await self.context.services.rabbitmq.connection
        channel = await connection.channel()
        queue = await channel.declare_queue(self.options.queue_name)

        self.logger.info("Processing queue %s", self.options.queue_name)
        async with queue.run() as q:
            async for message in q:
                yield message
