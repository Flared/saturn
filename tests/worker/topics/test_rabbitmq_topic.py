import typing as t

import asyncio
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from datetime import timedelta

import aio_pika
import asyncstdlib as alib
import pamqp.commands
import pamqp.constants
import pytest

from saturn_engine.config import Config
from saturn_engine.core import TopicMessage
from saturn_engine.utils import utcnow
from saturn_engine.worker.services.manager import ServicesManager
from saturn_engine.worker.services.rabbitmq import RabbitMQService
from saturn_engine.worker.topics import RabbitMQTopic
from saturn_engine.worker.topics.rabbitmq import RabbitMQSerializer
from tests.utils.tcp_proxy import TcpProxy


async def ensure_clean_queue(
    queue_name: str, *, connection: aio_pika.Connection
) -> None:
    async with connection.channel(on_return_raises=True) as channel:
        await channel.queue_delete(queue_name)


async def unwrap(context: t.AsyncContextManager[TopicMessage]) -> TopicMessage:
    async with context as message:
        return message


@pytest.fixture
async def topic_maker(
    rabbitmq_service_loader: t.Callable[..., Awaitable[RabbitMQService]],
    services_manager: ServicesManager,
) -> AsyncIterator[t.Callable[..., Awaitable[RabbitMQTopic]]]:
    queue_names = set()
    topics = []

    async def maker(
        services_manager: ServicesManager = services_manager,
        queue_name: str = "test",
        **kwargs: t.Any
    ) -> RabbitMQTopic:
        rabbitmq_service = await rabbitmq_service_loader(services_manager)

        if queue_name not in queue_names:
            await ensure_clean_queue(
                queue_name,
                connection=await rabbitmq_service.connection,
            )

        kwargs.setdefault("auto_delete", True)
        kwargs.setdefault("durable", False)
        options = RabbitMQTopic.Options(queue_name=queue_name, **kwargs)
        topic = RabbitMQTopic(options, services=services_manager.services)
        topics.append(topic)
        queue_names.add(queue_name)
        return topic

    yield maker

    for topic in topics:
        await topic.close()


@pytest.mark.asyncio
async def test_rabbitmq_topic(
    topic_maker: t.Callable[..., Awaitable[RabbitMQTopic]]
) -> None:
    topic = await topic_maker()

    messages = [
        TopicMessage(id="0", args={"n": 1}),
        TopicMessage(id="1", args={"n": 2}),
    ]

    for message in messages:
        await topic.publish(message, wait=True)

    async with alib.scoped_iter(topic.run()) as topic_iter:
        items = []
        async for context in alib.islice(topic_iter, 2):
            async with context as message:
                items.append(message)
        assert items == messages

    await topic.close()


@pytest.mark.asyncio
async def test_rabbitmq_topic_pickle(
    topic_maker: t.Callable[..., Awaitable[RabbitMQTopic]]
) -> None:
    topic = await topic_maker(serializer=RabbitMQSerializer.PICKLE)

    messages = [
        TopicMessage(id="0", args={"n": b"1", "time": utcnow()}),
        TopicMessage(id="1", args={"n": b"2", "time": utcnow()}),
    ]

    for message in messages:
        await topic.publish(message, wait=True)

    async with alib.scoped_iter(topic.run()) as topic_iter:
        items = []
        async for context in alib.islice(topic_iter, 2):
            async with context as message:
                items.append(message)
        assert items == messages

    await topic.close()


@pytest.mark.asyncio
async def test_bounded_rabbitmq_topic_max_length(
    topic_maker: t.Callable[..., Awaitable[RabbitMQTopic]]
) -> None:
    topic = await topic_maker(max_length=2, prefetch_count=2)
    topic.RETRY_PUBLISH_DELAY = timedelta(seconds=0.1)

    message = TopicMessage(id="0", args={"n": 1})

    assert await topic.publish(message, wait=False)
    assert await topic.publish(message, wait=True)
    assert not await topic.publish(message, wait=False)
    publish_task = asyncio.create_task(topic.publish(message, wait=True))
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(asyncio.shield(publish_task), 0.5)

    async with alib.scoped_iter(topic.run()) as topic_iter:
        assert await unwrap(await alib.anext(topic_iter)) == message
        assert await publish_task

        # We can still publish two more message, because at that point 2
        # messages are waiting on the consumer buffer, so the queue is empty.
        assert await topic.publish(message, wait=True)
        assert await topic.publish(message, wait=True)

        # However one more and we fill the queue again.
        publish_task = asyncio.create_task(topic.publish(message, wait=True))
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(asyncio.shield(publish_task), 0.5)

        assert await unwrap(await alib.anext(topic_iter)) == message
        assert await unwrap(await alib.anext(topic_iter)) == message
        assert await publish_task

    await topic.close()


@pytest.mark.asyncio
async def test_rabbitmq_topic_channel_closed(
    services_manager_maker: t.Callable[[Config], t.Awaitable[ServicesManager]],
    config: Config,
    tcp_proxy: t.Callable[[int, int], Awaitable[TcpProxy]],
    topic_maker: t.Callable[..., Awaitable[RabbitMQTopic]],
    rabbitmq_service_loader: t.Callable[..., Awaitable[RabbitMQService]],
) -> None:
    proxy = await tcp_proxy(15672, 5672)
    config = config.load_object(
        {"rabbitmq": {"url": "amqp://127.0.0.1:15672/", "reconnect_interval": 1}}
    )
    services_manager = await services_manager_maker(config)

    async def close_all_channels() -> None:
        rabbitmq_service = await rabbitmq_service_loader(services_manager)
        connection = await rabbitmq_service.connection
        for channel in dict(connection.connection.channels).values():
            await channel.rpc(
                pamqp.commands.Channel.Close(
                    reply_code=pamqp.constants.REPLY_SUCCESS,
                    class_id=0,
                    method_id=0,
                ),
            )

    topic = await topic_maker(
        services_manager=services_manager, durable=True, auto_delete=False
    )

    message = TopicMessage(id="0", args={"n": 1})

    assert await topic.publish(message, wait=False)
    assert await topic.publish(message, wait=False)

    await proxy.disconnect()
    with pytest.raises(Exception):
        assert await topic.publish(message, wait=False)
    assert await topic.publish(message, wait=True)

    await close_all_channels()
    with pytest.raises(Exception):
        assert await topic.publish(message, wait=False)
    assert await topic.publish(message, wait=True)

    await topic.close()


@pytest.mark.asyncio
async def test_closed_rabbitmq_topic(
    topic_maker: t.Callable[..., Awaitable[RabbitMQTopic]]
) -> None:
    topic = await topic_maker()
    await topic.close()
    with pytest.raises(Exception):
        await topic.publish(TopicMessage(id="0", args={"n": 0}), wait=True)
