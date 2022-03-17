import typing as t

import asyncio
import contextlib
import dataclasses
import json
from collections.abc import AsyncGenerator
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import timedelta

import aio_pika
import aio_pika.exceptions
import asyncstdlib as alib
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

    RETRY_PUBLISH_DELAY = timedelta(seconds=1)
    FAILURE_RETRY_DELAY = timedelta(seconds=30)

    @dataclasses.dataclass
    class Options:
        queue_name: str
        auto_delete: bool = False
        durable: bool = True
        max_length: t.Optional[int] = None
        prefetch_count: t.Optional[int] = None

    class TopicServices:
        rabbitmq: RabbitMQService

    def __init__(self, options: Options, services: Services, **kwargs: object) -> None:
        self.logger = getLogger(__name__, self)
        self.options = options
        self.services = services.cast(RabbitMQTopic.TopicServices)
        self.exit_stack = contextlib.AsyncExitStack()

    async def run(self) -> AsyncGenerator[t.AsyncContextManager[TopicMessage], None]:
        self.logger.info("Starting queue %s", self.options.queue_name)
        while True:
            try:
                self.logger.info("Processing queue %s", self.options.queue_name)
                async with (await self.queue).iterator() as queue_iter:
                    async for message in queue_iter:
                        yield self.message_context(message)
            except Exception:
                self.logger.exception("RabbitMQ topic failed")
                await asyncio.sleep(self.FAILURE_RETRY_DELAY.total_seconds())
            else:
                break

    async def publish(self, message: TopicMessage, wait: bool) -> bool:
        await self.queue  # Ensure the queue is created.
        exchange = (await self.channel).default_exchange
        while True:
            try:
                await exchange.publish(
                    aio_pika.Message(
                        body=json.dumps(asdict(message)).encode(),
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    ),
                    routing_key=self.options.queue_name,
                )
                return True
            except aio_pika.exceptions.ChannelInvalidStateError:
                # The channel is broken. Let's create a new one.
                await self.reset()
                self.logger.exception("Channel error on publish")
            except aio_pika.exceptions.DeliveryError as e:
                # Only handle Nack
                if e.frame.name != "Basic.Nack":
                    raise

            # If we are non blocking, we stop trying.
            if not wait:
                return False

            # Otherwise, take a small break and try again.
            await asyncio.sleep(self.RETRY_PUBLISH_DELAY.total_seconds())

    async def reset(self) -> None:
        await self.close()
        del self.channel
        del self.queue

    @asynccontextmanager
    async def message_context(
        self, message: IncomingMessage
    ) -> AsyncIterator[TopicMessage]:
        async with message.process():
            yield fromdict(json.loads(message.body.decode()), TopicMessage)

    @alib.cached_property
    async def channel(self) -> aio_pika.Channel:
        connection: aio_pika.Connection = await self.services.rabbitmq.connection
        channel = await self.exit_stack.enter_async_context(
            connection.channel(on_return_raises=True)
        )
        if self.options.prefetch_count is not None:
            await channel.set_qos(prefetch_count=self.options.prefetch_count)
        return channel

    @alib.cached_property
    async def queue(self) -> aio_pika.Queue:
        arguments: dict[str, t.Any] = {}
        if self.options.max_length:
            arguments["x-max-length"] = self.options.max_length
            arguments["x-overflow"] = "reject-publish"

        queue = await (await self.channel).declare_queue(
            self.options.queue_name,
            auto_delete=self.options.auto_delete,
            durable=self.options.durable,
            arguments=arguments,
        )
        return queue

    async def close(self) -> None:
        await self.exit_stack.aclose()
