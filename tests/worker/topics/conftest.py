import typing as t

from collections.abc import AsyncIterator
from collections.abc import Awaitable

import aio_pika.abc
import pytest

from saturn_engine.worker.services.manager import ServicesManager
from saturn_engine.worker.services.rabbitmq import RabbitMQService
from saturn_engine.worker.topics.rabbitmq import RabbitMQTopic

T = t.TypeVar("T", bound=RabbitMQTopic)


class RabbitMQTopicMaker(t.Protocol):
    async def __call__(self, klass: t.Type[T], **kwargs: t.Any) -> T: ...


async def ensure_clean_queue(
    queue_name: str, *, connection: aio_pika.abc.AbstractRobustConnection
) -> None:
    async with connection.channel(on_return_raises=True) as channel:
        await channel.queue_delete(queue_name)


@pytest.fixture
async def rabbitmq_topic_maker(
    rabbitmq_service_loader: t.Callable[..., Awaitable[RabbitMQService]],
    services_manager: ServicesManager,
) -> AsyncIterator[RabbitMQTopicMaker]:
    queue_names = set()
    topics = []

    async def maker(
        klass: t.Type[T],
        services_manager: ServicesManager = services_manager,
        queue_name: str = "test",
        **kwargs: t.Any,
    ) -> T:
        rabbitmq_service = await rabbitmq_service_loader(services_manager)

        if queue_name not in queue_names:
            await ensure_clean_queue(
                queue_name,
                connection=await rabbitmq_service.connections.get("default"),
            )

        kwargs.setdefault("auto_delete", True)
        kwargs.setdefault("durable", False)
        options = klass.Options(queue_name=queue_name, **kwargs)
        topic = klass(options, services=services_manager.services)
        topic.name = queue_name
        topics.append(topic)
        queue_names.add(queue_name)
        return topic

    yield maker

    for topic in topics:
        await topic.close()
