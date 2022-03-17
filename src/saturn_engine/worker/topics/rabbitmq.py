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
    FAILURE_RETRY_BACKOFFS = [timedelta(seconds=s) for s in (0, 1, 5, 15, 30)]

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
        self._channel: t.Optional[aio_pika.Channel] = None
        self._queue: t.Optional[aio_pika.Queue] = None

    async def run(self) -> AsyncGenerator[t.AsyncContextManager[TopicMessage], None]:
        self.logger.info("Starting queue %s", self.options.queue_name)
        attempt = 0
        while True:
            try:
                self.logger.info("Processing queue %s", self.options.queue_name)
                async with (await self.get_queue()).iterator() as queue_iter:
                    async for message in queue_iter:
                        attempt = 0
                        yield self.message_context(message)
            except Exception:
                self.logger.exception("Failed to consume")
                await self.reset()
                await self.backoff_sleep(attempt)
                attempt += 1
            else:
                break

    async def publish(
        self,
        message: TopicMessage,
        wait: bool,
    ) -> bool:
        attempt = 0
        while True:
            try:
                await self.get_queue()  # Ensure the queue is created.
                exchange = (await self.get_channel()).default_exchange
                if exchange is None:
                    raise ValueError("Channel has no exchange")
                await exchange.publish(
                    aio_pika.Message(
                        body=json.dumps(asdict(message)).encode(),
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    ),
                    routing_key=self.options.queue_name,
                )
                return True
            except (aio_pika.exceptions.ChannelInvalidStateError, ConnectionError):
                self.logger.exception("Failed to publish")

                # The channel is broken. Let's create a new one.
                await self.reset()

                # If we are non blocking, we try again only once to deal with
                # possible connection reset that are quick to retry.
                if wait or attempt == 0:
                    await self.backoff_sleep(attempt)
                else:
                    # otherwise we reraise the failure.
                    raise
                attempt += 1

            except aio_pika.exceptions.DeliveryError as e:
                # Only handle Nack
                if e.frame.name != "Basic.Nack":
                    raise

                # If we are non blocking, we stop trying.
                if not wait:
                    return False

                # Otherwise, take a small break and try again.
                await asyncio.sleep(self.RETRY_PUBLISH_DELAY.total_seconds())
                attempt = 0

    async def backoff_sleep(self, attempt: int) -> None:
        retry_delay = self.FAILURE_RETRY_BACKOFFS[-1]
        if attempt < len(self.FAILURE_RETRY_BACKOFFS):
            retry_delay = self.FAILURE_RETRY_BACKOFFS[attempt]
        await asyncio.sleep(retry_delay.total_seconds())

    async def reset(self) -> None:
        await self.close()
        self._channel = None
        self._queue = None

    @asynccontextmanager
    async def message_context(
        self, message: IncomingMessage
    ) -> AsyncIterator[TopicMessage]:
        async with message.process():
            yield fromdict(json.loads(message.body.decode()), TopicMessage)

    async def get_channel(self) -> aio_pika.Channel:
        if self._channel is None:
            connection: aio_pika.Connection = await self.services.rabbitmq.connection
            channel = await self.exit_stack.enter_async_context(
                connection.channel(on_return_raises=True)
            )

            if self.options.prefetch_count is not None:
                await channel.set_qos(prefetch_count=self.options.prefetch_count)
            self._channel = channel

        return self._channel

    async def get_queue(self) -> aio_pika.Queue:
        if self._queue is None:
            arguments: dict[str, t.Any] = {}
            if self.options.max_length:
                arguments["x-max-length"] = self.options.max_length
                arguments["x-overflow"] = "reject-publish"

            self._queue = await (await self.get_channel()).declare_queue(
                self.options.queue_name,
                auto_delete=self.options.auto_delete,
                durable=self.options.durable,
                arguments=arguments,
            )

        return self._queue

    async def close(self) -> None:
        await self.exit_stack.aclose()
