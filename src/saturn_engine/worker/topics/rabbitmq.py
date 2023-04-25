import typing as t

import asyncio
import contextlib
import dataclasses
import enum
import json
import pickle  # noqa: S403
from collections.abc import AsyncGenerator
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import timedelta

import aio_pika
import aio_pika.abc
import aio_pika.exceptions

from saturn_engine.core import TopicMessage
from saturn_engine.utils.asyncutils import SharedLock
from saturn_engine.utils.asyncutils import cached_property
from saturn_engine.utils.log import getLogger
from saturn_engine.utils.options import asdict
from saturn_engine.utils.options import fromdict
from saturn_engine.worker.services import Services
from saturn_engine.worker.services.rabbitmq import RabbitMQService

from ..topic import Topic
from ..topic import TopicClosedError


class RabbitMQSerializer(enum.Enum):
    JSON = "json"
    PICKLE = "pickle"

    @property
    def content_type(self) -> str:
        return SerializerToContentType[self]

    @classmethod
    def from_content_type(cls, content_type: str) -> "RabbitMQSerializer | None":
        return ContentTypeToSerializer.get(content_type)


SerializerToContentType = {
    RabbitMQSerializer.JSON: "application/json",
    RabbitMQSerializer.PICKLE: "application/python-pickle",
}

ContentTypeToSerializer: dict[str, RabbitMQSerializer] = {
    v: k for k, v in SerializerToContentType.items()
}


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
        max_length_bytes: t.Optional[int] = None
        overflow: t.Optional[str] = "reject-publish"
        prefetch_count: t.Optional[int] = None
        serializer: RabbitMQSerializer = RabbitMQSerializer.JSON
        log_above_size: t.Optional[int] = None

    class TopicServices:
        rabbitmq: RabbitMQService

    def __init__(self, options: Options, services: Services, **kwargs: object) -> None:
        self.logger = getLogger(__name__, self)
        self.options = options
        self.services = services.cast(RabbitMQTopic.TopicServices)
        self.exit_stack = contextlib.AsyncExitStack()
        self.is_closed = False
        self._queue: t.Optional[aio_pika.abc.AbstractQueue] = None
        self._publish_lock = SharedLock(max_reservations=8)

    async def run(self) -> AsyncGenerator[t.AsyncContextManager[TopicMessage], None]:
        if self.is_closed:
            raise TopicClosedError()

        self.logger.info("Starting queue %s", self.options.queue_name)
        attempt = 0
        while not self.is_closed:
            try:
                self.logger.info("Processing queue %s", self.options.queue_name)
                queue = await self.queue
                async with queue.iterator() as queue_iter:
                    async for message in queue_iter:
                        attempt = 0
                        yield self.message_context(message)
            except Exception:
                self.logger.exception("Failed to consume")
                await self.backoff_sleep(attempt)
                attempt += 1
            else:
                break

    async def publish(
        self,
        message: TopicMessage,
        wait: bool,
    ) -> bool:
        if self.is_closed:
            raise TopicClosedError()

        attempt = 0

        # Wait for the queue to unblock.
        if not wait and self._publish_lock.locked_reservations():
            return False

        async with self._publish_lock.reserve() as reservation:
            while True:
                try:
                    await self.ensure_queue()  # Ensure the queue is created.
                    channel = await self.channel
                    exchange = channel.default_exchange
                    if exchange is None:
                        raise ValueError("Channel has no exchange")

                    await exchange.publish(
                        aio_pika.Message(
                            body=self._serialize(message),
                            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                            content_type=self.options.serializer.content_type,
                        ),
                        routing_key=self.options.queue_name,
                    )
                    return True
                except aio_pika.exceptions.DeliveryError as e:
                    # Only handle Nack
                    if e.frame.name != "Basic.Nack":
                        raise

                    if not wait:
                        return False

                    # If the lock is being help by another task, we can assume
                    # that once the control is being yielded to this task it
                    # will be after successfully publishing a message, therefor
                    # there's no need to sleep.
                    has_locked = reservation.locked() or not self._publish_lock.locked()
                    await reservation.acquire()
                    if has_locked:
                        await asyncio.sleep(self.RETRY_PUBLISH_DELAY.total_seconds())
                    attempt = 0
                except Exception:
                    self.logger.exception("Failed to publish")
                    if not wait:
                        raise

                    # If the lock is being help by another task, we can assume
                    # that once the control is being yielded to this task it
                    # will be after successfully publishing a message, therefor
                    # there's no need to sleep.
                    has_locked = reservation.locked() or not self._publish_lock.locked()
                    await reservation.acquire()
                    if has_locked:
                        await self.backoff_sleep(attempt)
                        attempt += 1

            return False

    async def backoff_sleep(self, attempt: int) -> None:
        retry_delay = self.FAILURE_RETRY_BACKOFFS[-1]
        if attempt < len(self.FAILURE_RETRY_BACKOFFS):
            retry_delay = self.FAILURE_RETRY_BACKOFFS[attempt]
        await asyncio.sleep(retry_delay.total_seconds())

    @asynccontextmanager
    async def message_context(
        self, message: aio_pika.abc.AbstractIncomingMessage
    ) -> AsyncIterator[TopicMessage]:
        async with message.process():
            yield self._deserialize(message)

    @cached_property
    async def channel(self) -> aio_pika.abc.AbstractChannel:
        connection = await self.services.s.rabbitmq.connection
        channel = await self.exit_stack.enter_async_context(
            connection.channel(on_return_raises=True)
        )

        if self.options.prefetch_count is not None:
            await channel.set_qos(prefetch_count=self.options.prefetch_count)
        channel.close_callbacks.add(self.channel_closed)
        channel.reopen_callbacks.add(self.channel_reopened)
        return channel

    def channel_closed(
        self, channel: aio_pika.abc.AbstractChannel, reason: t.Optional[Exception]
    ) -> None:
        extra = {"data": {"topic": {"id": self.name}}}
        if isinstance(reason, BaseException):
            self.logger.error("Channel closed", exc_info=reason, extra=extra)
        elif reason:
            self.logger.error("Channel closed: %s", reason, extra=extra)

    def channel_reopened(self, channel: aio_pika.abc.AbstractChannel) -> None:
        self.logger.info(
            "Channel reopening", extra={"data": {"topic": {"id": self.name}}}
        )

    @cached_property
    async def queue(self) -> aio_pika.abc.AbstractQueue:
        arguments: dict[str, t.Any] = {}
        if self.options.max_length:
            arguments["x-max-length"] = self.options.max_length
        if self.options.max_length_bytes:
            arguments["x-max-length-bytes"] = self.options.max_length
        arguments["x-overflow"] = self.options.overflow

        channel = await self.channel
        queue = await channel.declare_queue(
            self.options.queue_name,
            auto_delete=self.options.auto_delete,
            durable=self.options.durable,
            arguments=arguments,
        )

        return queue

    async def ensure_queue(self) -> aio_pika.abc.AbstractQueue:
        return await self.queue

    async def close(self) -> None:
        self.is_closed = True
        await self.exit_stack.aclose()

    def _serialize(self, message: TopicMessage) -> bytes:
        if self.options.serializer == RabbitMQSerializer.PICKLE:
            serialized_message = pickle.dumps(message)
        else:
            serialized_message = json.dumps(asdict(message)).encode()

        serialized_message_len = len(serialized_message)
        if (
            self.options.log_above_size
            and self.options.log_above_size > serialized_message_len
        ):
            self.logger.warning(
                "Sending large message",
                extra={
                    "data": {
                        "message": {
                            "id": message.id,
                            "size": serialized_message_len,
                        },
                        "topic": {"id": self.name},
                    }
                },
            )
        return serialized_message

    def _deserialize(
        self, message: aio_pika.abc.AbstractIncomingMessage
    ) -> TopicMessage:
        serializer = self.options.serializer
        if message.content_type:
            message_serializer = RabbitMQSerializer.from_content_type(
                message.content_type
            )
            if message_serializer:
                serializer = message_serializer

        if serializer is RabbitMQSerializer.PICKLE:
            deserialized = pickle.loads(message.body)  # noqa: S301
            if not isinstance(deserialized, TopicMessage):
                raise Exception("Deserialized RabbitMQ message is not a TopicMessage.")
            return deserialized

        return fromdict(json.loads(message.body.decode()), TopicMessage)
