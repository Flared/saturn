import typing as t

import asyncio
from collections.abc import AsyncIterator
from collections.abc import Awaitable

import aio_pika
import asyncstdlib as alib
import pytest

from saturn_engine.core import TopicMessage
from saturn_engine.worker.services.manager import ServicesManager
from saturn_engine.worker.services.rabbitmq import RabbitMQService
from saturn_engine.worker.topics import RabbitMQTopic
from tests.utils import TimeForwardLoop


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
    event_loop: TimeForwardLoop,
    rabbitmq_service: RabbitMQService,
    services_manager: ServicesManager,
) -> AsyncIterator[t.Callable[..., Awaitable[RabbitMQTopic]]]:
    event_loop.forward_time = False
    queue_names = set()
    topics = []

    async def maker(queue_name: str = "test", **kwargs: t.Any) -> RabbitMQTopic:
        if queue_name not in queue_names:
            await ensure_clean_queue(
                queue_name,
                connection=await services_manager.services.rabbitmq.connection,
            )

        options = RabbitMQTopic.Options(
            queue_name=queue_name, auto_delete=True, durable=False, **kwargs
        )
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
async def test_bounded_rabbitmq_topic_max_length(
    event_loop: TimeForwardLoop, topic_maker: t.Callable[..., Awaitable[RabbitMQTopic]]
) -> None:
    topic = await topic_maker(max_length=2, prefetch_count=2)
    topic.RETRY_PUBLISH_DELAY = 0.1

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
