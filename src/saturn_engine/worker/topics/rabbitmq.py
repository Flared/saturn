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
from saturn_engine.utils.lru import LRUDefaultDict
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


@dataclasses.dataclass
class Exchange:
    name: str
    type: aio_pika.abc.ExchangeType = aio_pika.abc.ExchangeType.DIRECT
    durable: bool = True
    auto_delete: bool = False
    exclusive: bool = False
    passive: bool = False
    arguments: dict[str, t.Any] = dataclasses.field(default_factory=dict)
    timeout: int | float | None = None


class RabbitMQTopic(Topic):
    """A queue that consume message from RabbitMQ"""

    RETRY_PUBLISH_DELAY = timedelta(seconds=10)
    FAILURE_RETRY_BACKOFFS = [timedelta(seconds=s) for s in (0, 5, 10, 20, 30, 60)]

    @dataclasses.dataclass
    class Options:
        queue_name: str
        connection_name: str = "default"
        auto_delete: bool = False
        durable: bool = True
        max_length: t.Optional[int] = None
        max_length_bytes: t.Optional[int] = None
        overflow: t.Optional[str] = "reject-publish"
        prefetch_count: t.Optional[int] = 1
        serializer: RabbitMQSerializer = RabbitMQSerializer.JSON
        log_above_size: t.Optional[int] = None
        max_publish_concurrency: int = 8
        max_retry: int | None = None
        arguments: dict[str, t.Any] = dataclasses.field(default_factory=dict)
        exchange: Exchange | None = None
        routing_key: str | None = None

    class TopicServices:
        rabbitmq: RabbitMQService

    def __init__(self, options: Options, services: Services, **kwargs: object) -> None:
        self.logger = getLogger(__name__, self)
        self.options = options
        self.services = services.cast(RabbitMQTopic.TopicServices)
        self.exit_stack = contextlib.AsyncExitStack()
        self.is_closed = False
        self._queue: t.Optional[aio_pika.abc.AbstractQueue] = None
        self._publish_lock = SharedLock(
            max_reservations=options.max_publish_concurrency
        )
        self.attempt_by_message: LRUDefaultDict[str, int] = LRUDefaultDict(
            cache_len=1024, default_factory=lambda: 0
        )
        self.queue_arguments: dict[str, t.Any] = self.options.arguments

        if self.options.max_length:
            self.queue_arguments.setdefault("x-max-length", self.options.max_length)
        if self.options.max_length_bytes:
            self.queue_arguments.setdefault(
                "x-max-length-bytes", self.options.max_length_bytes
            )
        self.queue_arguments.setdefault("x-overflow", self.options.overflow)

    async def open(self) -> None:
        await self.ensure_queue()

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
                body = self._serialize(message)
                try:
                    await self.ensure_queue()  # Ensure the queue is created.
                    exchange = await self.exchange
                    await exchange.publish(
                        aio_pika.Message(
                            body=body,
                            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                            content_type=self.options.serializer.content_type,
                            expiration=message.expire_after,
                        ),
                        routing_key=self.options.routing_key or self.options.queue_name,
                    )
                    return True
                except aio_pika.exceptions.DeliveryError as e:
                    # Only handle Nack
                    if e.frame.name != "Basic.Nack":
                        raise

                    if isinstance(e, aio_pika.exceptions.PublishError):
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
                        if not await self.backoff_sleep(attempt):
                            raise
                        attempt += 1

            return False

    async def backoff_sleep(self, attempt: int) -> bool:
        if attempt >= len(self.FAILURE_RETRY_BACKOFFS):
            return False
        retry_delay = self.FAILURE_RETRY_BACKOFFS[attempt]
        await asyncio.sleep(retry_delay.total_seconds())
        return True

    @asynccontextmanager
    async def message_context(
        self, message: aio_pika.abc.AbstractIncomingMessage
    ) -> AsyncIterator[TopicMessage]:
        requeue: bool = False
        if message.message_id and self.options.max_retry:
            requeue = (
                self.attempt_by_message.get(message.message_id, 0)
                < self.options.max_retry
            )
        async with message.process(requeue=requeue):
            try:
                yield self._deserialize(message)
            except Exception:
                if message.message_id and self.options.max_retry:
                    self.attempt_by_message[message.message_id] += 1
                raise
            if message.message_id:
                self.attempt_by_message.pop(message.message_id, None)

    @cached_property
    async def channel(self) -> aio_pika.abc.AbstractChannel:
        connection = await self.services.s.rabbitmq.connections.get(
            self.options.connection_name
        )
        channel: aio_pika.abc.AbstractRobustChannel = await self.exit_stack.enter_async_context(
            connection.channel(on_return_raises=True)  # type: ignore[arg-type]
        )

        if self.options.prefetch_count is not None:
            await channel.set_qos(prefetch_count=self.options.prefetch_count)
        channel.close_callbacks.add(self.channel_closed)
        channel.reopen_callbacks.add(self.channel_reopened)
        return channel

    @cached_property
    async def exchange(self) -> aio_pika.abc.AbstractExchange:
        channel = await self.channel
        if self.options.exchange:
            return await channel.declare_exchange(
                name=self.options.exchange.name,
                type=self.options.exchange.type,
                durable=self.options.exchange.durable,
                passive=self.options.exchange.passive,
                exclusive=self.options.exchange.exclusive,
                auto_delete=self.options.exchange.auto_delete,
                arguments=self.options.exchange.arguments,
                timeout=self.options.exchange.timeout,
            )
        return channel.default_exchange

    async def ensure_exchange(self) -> aio_pika.abc.AbstractExchange:
        return await self.exchange

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
        channel = await self.channel
        queue = await channel.declare_queue(
            self.options.queue_name,
            auto_delete=self.options.auto_delete,
            durable=self.options.durable,
            arguments=self.queue_arguments,
        )
        await self.ensure_exchange()
        if self.options.exchange:
            await queue.bind(
                self.options.exchange.name,
                routing_key=self.options.routing_key or self.options.queue_name,
            )
        elif self.options.routing_key:
            await queue.bind("", routing_key=self.options.routing_key)

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
            and self.options.log_above_size < serialized_message_len
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
